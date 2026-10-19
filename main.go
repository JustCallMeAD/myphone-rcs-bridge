// myphone-rcs-bridge connects My Phone (a Windows app) to Google Messages the way Google Messages
// for web does, so RCS chats can be read and answered from the PC. It is a small helper program:
// My Phone starts it, talks to it through standard input and output (one JSON object per line),
// and keeps the pairing itself. It uses libgm from mautrix-gmessages and is licensed, like it,
// under the GNU Affero General Public License v3.0 or later (see LICENSE).
//
// Nothing a message says, and no phone number or name, is ever written to the log (standard error).
package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exhttp"

	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

// Version is reported to My Phone in the "ready" event.
const Version = "1.0.0"

// ---------------------------------------------------------------- Wire format

// Command is one line from My Phone.
type Command struct {
	ID             int64             `json:"id"`
	Cmd            string            `json:"cmd"`
	Session        json.RawMessage   `json:"session,omitempty"`
	ConversationID string            `json:"conversationId,omitempty"`
	MessageID      string            `json:"messageId,omitempty"`
	Text           string            `json:"text,omitempty"`
	Limit          int64             `json:"limit,omitempty"`
	MediaID        string            `json:"mediaId,omitempty"`
	Cookies        map[string]string `json:"cookies,omitempty"`
	Device         int               `json:"device,omitempty"`
	Key            string            `json:"key,omitempty"`
	Data           string            `json:"data,omitempty"`      // base64, for sendMedia
	Name           string            `json:"name,omitempty"`      // file name, for sendMedia
	Mime           string            `json:"mime,omitempty"`      // for sendMedia
	Emoji          string            `json:"emoji,omitempty"`     // for react
	Remove         bool              `json:"remove,omitempty"`    // react: take the reaction off
	ReplyTo        string            `json:"replyTo,omitempty"`   // send: the message answered
	SIM            string            `json:"sim,omitempty"`       // send: the SIM's participant id
	Numbers        []string          `json:"numbers,omitempty"`   // newChat
	GroupName      string            `json:"groupName,omitempty"` // newChat, several numbers
	Typing         bool              `json:"typing,omitempty"`    // setTyping
	Archive        bool              `json:"archive,omitempty"`   // archive: false brings it back
	Phone          string            `json:"phone,omitempty"`     // deleteConversation
}

// Participant is a person in a conversation.
type Participant struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Number string `json:"number"`
	IsMe   bool   `json:"isMe"`
}

// Conversation is one conversation in Google Messages.
type Conversation struct {
	ID            string        `json:"id"`
	Name          string        `json:"name"`
	IsRCS         bool          `json:"isRcs"`
	IsGroup       bool          `json:"isGroup"`
	Unread        bool          `json:"unread"`
	LastMs        int64         `json:"lastMs"`
	Snippet       string        `json:"snippet"`
	SnippetFromMe bool          `json:"snippetFromMe"`
	ReadOnly      bool          `json:"readOnly"`
	Archived      bool          `json:"archived"` // in Google Messages' Archived
	Participants  []Participant `json:"participants"`
	// The SIM (its participant id) Google Messages sends from in this conversation.
	DefaultSIM string `json:"defaultSim"`
}

// Reaction is one emoji on a message, with how many people chose it.
type Reaction struct {
	Emoji string `json:"emoji"`
	Count int    `json:"count"`
	Mine  bool   `json:"mine"`
}

// SIM is one of the phone's SIM cards.
type SIM struct {
	ID      string `json:"id"`
	Carrier string `json:"carrier"`
	Number  string `json:"number"`
}

// Media is a picture, video or file in a message.
type Media struct {
	MediaID string `json:"mediaId"`
	Key     string `json:"key"`
	Mime    string `json:"mime"`
	Name    string `json:"name"`
	Size    int64  `json:"size"`
}

// Message is one message.
type Message struct {
	ID             string     `json:"id"`
	ConversationID string     `json:"conversationId"`
	SenderName     string     `json:"senderName"`
	FromMe         bool       `json:"fromMe"`
	TimeMs         int64      `json:"timeMs"`
	Text           string     `json:"text"`
	Status         string     `json:"status"`
	Media          []Media    `json:"media,omitempty"`
	Reactions      []Reaction `json:"reactions,omitempty"`
	ReplyTo        string     `json:"replyTo,omitempty"`
}

// ---------------------------------------------------------------- Output

var out = struct {
	sync.Mutex
	w *bufio.Writer
}{w: bufio.NewWriter(os.Stdout)}

func send(v map[string]any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	out.Lock()
	defer out.Unlock()
	out.w.Write(data)
	out.w.WriteByte('\n')
	out.w.Flush()
}

// progress tells My Phone (for its log) where a long step has got to. Never personal data.
func progress(text string) {
	event("log", map[string]any{"text": text})
}

func event(name string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["event"] = name
	send(fields)
}

func reply(id int64, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["id"] = id
	fields["ok"] = true
	send(fields)
}

func fail(id int64, code string, err error) {
	message := code
	if err != nil {
		message = err.Error()
	}
	send(map[string]any{"id": id, "ok": false, "code": code, "message": message})
}

// ---------------------------------------------------------------- Bridge

type bridge struct {
	log    zerolog.Logger
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	client *libgm.Client
	sims   map[string]*gmproto.SIMCard // by the SIM's own participant id
	convs  map[string]*gmproto.Conversation
	self   map[string]bool // this phone's own participant ids (its SIMs)
	paired chan *gmproto.PairedData
}

func newBridge(log zerolog.Logger) *bridge {
	ctx, cancel := context.WithCancel(context.Background())
	return &bridge{log: log, ctx: ctx, cancel: cancel, sims: map[string]*gmproto.SIMCard{}, convs: map[string]*gmproto.Conversation{}, self: map[string]bool{}}
}

func (b *bridge) newClient(auth *libgm.AuthData) *libgm.Client {
	cli := libgm.NewClient(auth, nil, b.log.With().Str("component", "libgm").Logger(), exhttp.SensibleClientSettings)
	cli.SetEventHandler(b.onEvent)
	return cli
}

// start connects with a stored session, or pairs anew (QR codes) when there is none.
func (b *bridge) start(cmd Command) {
	var auth *libgm.AuthData
	if len(cmd.Session) > 0 && string(cmd.Session) != "null" {
		auth = libgm.NewAuthData()
		if err := json.Unmarshal(cmd.Session, auth); err != nil {
			fail(cmd.ID, "bad_session", err)
			return
		}
	}
	if auth == nil || (auth.Browser == nil && !auth.IsGoogleAccount()) {
		reply(cmd.ID, map[string]any{"pairing": true})
		go b.pair()
		return
	}
	b.mu.Lock()
	b.client = b.newClient(auth)
	cli := b.client
	b.mu.Unlock()
	reply(cmd.ID, map[string]any{"pairing": false})
	go b.connect(cli)
}

func (b *bridge) connect(cli *libgm.Client) {
	if err := cli.FetchConfig(b.ctx); err != nil {
		b.log.Warn().Err(err).Msg("config fetch failed")
	}
	err := cli.Connect(b.ctx)
	switch {
	case err == nil:
		event("connected", nil)
	case errors.Is(err, events.ErrRequestedEntityNotFound), errors.Is(err, events.ErrInvalidCredentials):
		event("unpaired", map[string]any{"reason": "Google Messages no longer knows this PC. Pair again."})
	default:
		event("disconnected", map[string]any{"reason": "Could not reach Google Messages: " + err.Error()})
	}
}

// pairGoogle pairs through the Google account the phone's Google Messages uses (phones that offer
// no QR code scanner): the cookies come from signing in to Google on the PC, then the phone asks the
// person to tap the emoji shown here.
func (b *bridge) pairGoogle(cmd Command) {
	if len(cmd.Cookies) == 0 {
		fail(cmd.ID, "no_cookies", errors.New("Sign in to Google first."))
		return
	}
	reply(cmd.ID, map[string]any{"pairing": true})
	go func() {
		auth := libgm.NewAuthData()
		auth.SetCookies(cmd.Cookies)
		cli := b.newClient(auth)
		// Several phones on one Google account: each try asks the next one (My Phone counts the tries).
		cli.GaiaHackyDeviceSwitcher = cmd.Device
		b.mu.Lock()
		b.client = cli
		b.mu.Unlock()
		if err := cli.FetchConfig(b.ctx); err != nil {
			event("pairFailed", map[string]any{"reason": "Could not reach Google Messages with that Google sign-in: " + err.Error()})
			return
		}
		progress("google pairing: asking the phone")
		emoji, session, err := cli.StartGaiaPairing(b.ctx, b.ctx)
		if err != nil {
			reason := "Could not start pairing: " + err.Error()
			switch {
			case errors.Is(err, libgm.ErrNoDevicesFound):
				reason = "No phone with Google Messages was found on that Google account. Sign in with the account the phone uses."
			case errors.Is(err, libgm.ErrPairingInitTimeout):
				reason = "The phone did not answer. Make sure it is online and Google Messages is up to date."
			}
			cli.Disconnect()
			event("pairFailed", map[string]any{"reason": reason})
			return
		}
		event("emoji", map[string]any{"emoji": emoji, "url": libgm.GetEmojiSVG(emoji)})
		progress("google pairing: emoji shown, waiting for the phone")
		// The phone has a few minutes to answer; after that, say so rather than wait for ever.
		finishCtx, cancel := context.WithTimeout(b.ctx, 4*time.Minute)
		_, err = cli.FinishGaiaPairing(finishCtx, session)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			cli.Disconnect()
			event("pairFailed", map[string]any{"reason": "The phone did not confirm in time.", "timeout": true})
			return
		}
		if err != nil {
			reason := "Pairing did not finish: " + err.Error()
			switch {
			case errors.Is(err, libgm.ErrIncorrectEmoji):
				reason = "A different emoji was tapped on the phone."
			case errors.Is(err, libgm.ErrPairingCancelled):
				reason = "Pairing was cancelled on the phone."
			case errors.Is(err, libgm.ErrPairingTimeout):
				reason = "No emoji was tapped on the phone in time."
			}
			cli.Disconnect()
			event("pairFailed", map[string]any{"reason": reason})
			return
		}
		progress("google pairing: confirmed")
		cli.Disconnect()
		b.sendSession(cli)
		event("paired", nil)
		b.mu.Lock()
		b.client = b.newClient(cli.AuthData)
		next := b.client
		b.mu.Unlock()
		b.connect(next)
	}()
}

// pair shows QR codes until the phone scans one (a new code every 30 seconds, for about 3 minutes).
func (b *bridge) pair() {
	cli := b.newClient(libgm.NewAuthData())
	b.mu.Lock()
	b.client = cli
	b.paired = make(chan *gmproto.PairedData, 1)
	paired := b.paired
	b.mu.Unlock()
	callback := func(data *gmproto.PairedData) { paired <- data }
	cli.PairCallback.Store(&callback)
	if err := cli.FetchConfig(b.ctx); err != nil {
		event("pairFailed", map[string]any{"reason": "Could not reach Google Messages: " + err.Error()})
		return
	}
	qr, err := cli.StartLogin(b.ctx)
	if err != nil {
		event("pairFailed", map[string]any{"reason": "Could not start pairing: " + err.Error()})
		return
	}
	event("qr", map[string]any{"url": qr})
	for attempt := 0; attempt < 6; attempt++ {
		select {
		case <-paired:
			// Give the phone a moment to save the pairing before connecting again.
			time.Sleep(2 * time.Second)
			cli.Disconnect()
			b.sendSession(cli)
			event("paired", nil)
			b.mu.Lock()
			b.client = b.newClient(cli.AuthData)
			next := b.client
			b.mu.Unlock()
			b.connect(next)
			return
		case <-time.After(30 * time.Second):
			next, err := cli.RefreshPhoneRelay()
			if err != nil {
				event("pairFailed", map[string]any{"reason": "Could not make a new code: " + err.Error()})
				cli.Disconnect()
				return
			}
			event("qr", map[string]any{"url": next})
		case <-b.ctx.Done():
			return
		}
	}
	cli.Disconnect()
	event("pairFailed", map[string]any{"reason": "No code was scanned in time."})
}

func (b *bridge) sendSession(cli *libgm.Client) {
	data, err := json.Marshal(cli.AuthData)
	if err == nil {
		event("session", map[string]any{"session": json.RawMessage(data)})
	}
}

func (b *bridge) current() *libgm.Client {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.client
}

func (b *bridge) onEvent(raw any) {
	switch evt := raw.(type) {
	case *events.ClientReady:
		b.mu.Lock()
		for _, c := range evt.Conversations {
			b.convs[c.GetConversationID()] = c
		}
		b.mu.Unlock()
		event("ready", map[string]any{"version": Version})
	case *gmproto.Conversation:
		b.mu.Lock()
		b.convs[evt.GetConversationID()] = evt
		b.mu.Unlock()
		event("conversation", map[string]any{"conversation": convertConversation(evt)})
	case *libgm.WrappedMessage:
		if evt.IsOld {
			return
		}
		event("message", map[string]any{"message": b.convertMessage(evt.Message)})
	case *gmproto.Settings:
		b.mu.Lock()
		for _, sim := range evt.GetSIMCards() {
			b.sims[sim.GetSIMParticipant().GetID()] = sim
			b.self[sim.GetSIMParticipant().GetID()] = true
		}
		b.mu.Unlock()
	case *events.AuthTokenRefreshed:
		if cli := b.current(); cli != nil {
			b.sendSession(cli)
		}
	case *gmproto.RevokePairData:
		event("unpaired", map[string]any{"reason": "This PC was unpaired in Google Messages."})
	case *events.GaiaLoggedOut:
		event("unpaired", map[string]any{"reason": "Signed out of Google Messages."})
	case *events.ListenFatalError:
		event("disconnected", map[string]any{"reason": "The connection to Google Messages stopped. It will try again."})
	case *events.ListenTemporaryError:
		event("state", map[string]any{"state": "retrying"})
	case *events.ListenRecovered:
		event("state", map[string]any{"state": "connected"})
	case *events.PhoneNotResponding:
		event("state", map[string]any{"state": "phoneNotResponding"})
	case *events.PhoneRespondingAgain:
		event("state", map[string]any{"state": "connected"})
	case *gmproto.TypingData:
		event("typing", map[string]any{
			"conversationId": evt.GetConversationID(),
			"number":         evt.GetUser().GetNumber(),
			"typing":         evt.GetType() == gmproto.TypingTypes_STARTED_TYPING,
		})
	case *events.BrowserActive:
		// Google Messages is open in a browser somewhere: only one web session is active at a time.
		event("state", map[string]any{"state": "otherSessionActive"})
	}
}

// ---------------------------------------------------------------- Requests

func (b *bridge) conversations(cmd Command) {
	cli := b.current()
	if cli == nil {
		fail(cmd.ID, "not_connected", nil)
		return
	}
	limit := cmd.Limit
	if limit <= 0 {
		limit = 75
	}
	resp, err := cli.ListConversations(b.ctx, &gmproto.ListConversationsRequest{Count: limit, Folder: gmproto.ListConversationsRequest_INBOX})
	if err != nil {
		fail(cmd.ID, "request_failed", err)
		return
	}
	convs := resp.GetConversations()
	// Archived ones too (marked archived), so they can be listed apart and brought back. Without
	// them the inbox is still worth answering with.
	if archived, err := cli.ListConversations(b.ctx, &gmproto.ListConversationsRequest{Count: limit, Folder: gmproto.ListConversationsRequest_ARCHIVE}); err == nil {
		convs = append(convs, archived.GetConversations()...)
	}
	list := make([]Conversation, 0, len(convs))
	seen := make(map[string]bool, len(convs))
	b.mu.Lock()
	for _, c := range convs {
		if seen[c.GetConversationID()] {
			continue
		}
		seen[c.GetConversationID()] = true
		b.convs[c.GetConversationID()] = c
		list = append(list, convertConversation(c))
	}
	b.mu.Unlock()
	reply(cmd.ID, map[string]any{"conversations": list})
}

func (b *bridge) messages(cmd Command) {
	cli := b.current()
	if cli == nil {
		fail(cmd.ID, "not_connected", nil)
		return
	}
	limit := cmd.Limit
	if limit <= 0 {
		limit = 100
	}
	resp, err := cli.FetchMessages(b.ctx, cmd.ConversationID, limit, nil)
	if err != nil {
		fail(cmd.ID, "request_failed", err)
		return
	}
	list := make([]Message, 0, len(resp.GetMessages()))
	for _, m := range resp.GetMessages() {
		list = append(list, b.convertMessage(m))
	}
	reply(cmd.ID, map[string]any{"messages": list})
}

func (b *bridge) sendText(cmd Command) {
	b.sendInfo(cmd, []*gmproto.MessageInfo{{
		Data: &gmproto.MessageInfo_MessageContent{MessageContent: &gmproto.MessageContent{Content: cmd.Text}},
	}})
}

// sendMedia uploads a picture, video or file, then sends it (with any text as its caption).
func (b *bridge) sendMedia(cmd Command) {
	cli := b.current()
	if cli == nil {
		fail(cmd.ID, "not_connected", nil)
		return
	}
	data, err := base64.StdEncoding.DecodeString(cmd.Data)
	if err != nil || len(data) == 0 {
		fail(cmd.ID, "bad_media", errors.New("The file could not be read."))
		return
	}
	media, err := cli.UploadMedia(data, cmd.Name, cmd.Mime)
	if err != nil {
		fail(cmd.ID, "upload_failed", err)
		return
	}
	info := []*gmproto.MessageInfo{{Data: &gmproto.MessageInfo_MediaContent{MediaContent: media}}}
	if cmd.Text != "" {
		info = append(info, &gmproto.MessageInfo{Data: &gmproto.MessageInfo_MessageContent{MessageContent: &gmproto.MessageContent{Content: cmd.Text}}})
	}
	b.sendInfo(cmd, info)
}

func (b *bridge) sendInfo(cmd Command, info []*gmproto.MessageInfo) {
	cli := b.current()
	if cli == nil {
		fail(cmd.ID, "not_connected", nil)
		return
	}
	b.mu.Lock()
	conv := b.convs[cmd.ConversationID]
	b.mu.Unlock()
	if conv == nil {
		got, err := cli.GetConversation(b.ctx, cmd.ConversationID)
		if err != nil {
			fail(cmd.ID, "no_conversation", err)
			return
		}
		conv = got
	}
	outgoing := conv.GetDefaultOutgoingID()
	b.mu.Lock()
	// Another SIM, when the person picked one (dual-SIM phones).
	if chosen, ok := b.sims[cmd.SIM]; ok && cmd.SIM != "" {
		outgoing = chosen.GetSIMParticipant().GetID()
	}
	sim := b.sims[outgoing]
	b.mu.Unlock()
	tmp := fmt.Sprintf("myphone-%d", time.Now().UnixNano())
	req := &gmproto.SendMessageRequest{
		ConversationID: cmd.ConversationID,
		MessagePayload: &gmproto.MessagePayload{
			TmpID:          tmp,
			ConversationID: cmd.ConversationID,
			ParticipantID:  outgoing,
			TmpID2:         tmp,
			MessageInfo:    info,
		},
		SIMPayload: sim.GetSIMData().GetSIMPayload(),
		TmpID:      tmp,
	}
	if cmd.ReplyTo != "" {
		req.Reply = &gmproto.ReplyPayload{MessageID: cmd.ReplyTo}
	}
	resp, err := cli.SendMessage(b.ctx, req)
	if err != nil {
		fail(cmd.ID, "send_failed", err)
		return
	}
	if resp.GetStatus() != gmproto.SendMessageResponse_SUCCESS {
		fail(cmd.ID, "send_failed", fmt.Errorf("Google Messages did not take the message (%s)", resp.GetStatus().String()))
		return
	}
	reply(cmd.ID, nil)
}

// react adds (or, with remove, takes off) an emoji reaction on a message.
func (b *bridge) react(cmd Command) {
	cli := b.current()
	if cli == nil {
		fail(cmd.ID, "not_connected", nil)
		return
	}
	action := gmproto.SendReactionRequest_ADD
	if cmd.Remove {
		action = gmproto.SendReactionRequest_REMOVE
	}
	b.mu.Lock()
	var payload *gmproto.SIMPayload
	if conv := b.convs[cmd.ConversationID]; conv != nil {
		payload = b.sims[conv.GetDefaultOutgoingID()].GetSIMData().GetSIMPayload()
	}
	b.mu.Unlock()
	resp, err := cli.SendReaction(b.ctx, &gmproto.SendReactionRequest{
		MessageID:    cmd.MessageID,
		ReactionData: gmproto.MakeReactionData(cmd.Emoji),
		Action:       action,
		SIMPayload:   payload,
	})
	if err != nil {
		fail(cmd.ID, "request_failed", err)
		return
	}
	if !resp.GetSuccess() {
		fail(cmd.ID, "request_failed", errors.New("Google Messages did not take the reaction."))
		return
	}
	reply(cmd.ID, nil)
}

// setTyping tells the other people "typing..." (Google Messages stops showing it by itself).
func (b *bridge) setTyping(cmd Command) {
	cli := b.current()
	if cli == nil || !cmd.Typing {
		reply(cmd.ID, nil)
		return
	}
	b.mu.Lock()
	var payload *gmproto.SIMPayload
	if conv := b.convs[cmd.ConversationID]; conv != nil {
		payload = b.sims[conv.GetDefaultOutgoingID()].GetSIMData().GetSIMPayload()
	}
	b.mu.Unlock()
	if err := cli.SetTyping(b.ctx, cmd.ConversationID, payload); err != nil {
		fail(cmd.ID, "request_failed", err)
		return
	}
	reply(cmd.ID, nil)
}

// newChat finds or starts a conversation with one number, or a group (a chat group when
// Google Messages can, else a group text) with several.
func (b *bridge) newChat(cmd Command) {
	cli := b.current()
	if cli == nil {
		fail(cmd.ID, "not_connected", nil)
		return
	}
	if len(cmd.Numbers) == 0 {
		fail(cmd.ID, "no_numbers", errors.New("Choose who to message."))
		return
	}
	req := &gmproto.GetOrCreateConversationRequest{}
	for _, number := range cmd.Numbers {
		req.Numbers = append(req.Numbers, &gmproto.ContactNumber{MysteriousInt: 2, Number: number, Number2: number})
	}
	if len(cmd.Numbers) > 1 && cmd.GroupName != "" {
		name := cmd.GroupName
		req.RCSGroupName = &name
	}
	resp, err := cli.GetOrCreateConversation(b.ctx, req)
	if err == nil && resp.GetStatus() == gmproto.GetOrCreateConversationResponse_CREATE_RCS {
		if req.RCSGroupName == nil {
			empty := ""
			req.RCSGroupName = &empty
		}
		create := true
		req.CreateRCSGroup = &create
		resp, err = cli.GetOrCreateConversation(b.ctx, req)
	}
	if err != nil {
		fail(cmd.ID, "request_failed", err)
		return
	}
	conv := resp.GetConversation()
	if conv.GetConversationID() == "" {
		fail(cmd.ID, "request_failed", errors.New("Google Messages did not start the conversation."))
		return
	}
	b.mu.Lock()
	b.convs[conv.GetConversationID()] = conv
	b.mu.Unlock()
	reply(cmd.ID, map[string]any{"conversation": convertConversation(conv)})
}

// deleteMessage deletes one message, on the phone too.
func (b *bridge) deleteMessage(cmd Command) {
	cli := b.current()
	if cli == nil {
		fail(cmd.ID, "not_connected", nil)
		return
	}
	resp, err := cli.DeleteMessage(b.ctx, cmd.MessageID)
	if err != nil {
		fail(cmd.ID, "request_failed", err)
		return
	}
	if !resp.GetSuccess() {
		fail(cmd.ID, "request_failed", errors.New("Google Messages did not delete it."))
		return
	}
	reply(cmd.ID, nil)
}

// deleteConversation deletes a whole conversation, on the phone too.
func (b *bridge) deleteConversation(cmd Command) {
	cli := b.current()
	if cli == nil {
		fail(cmd.ID, "not_connected", nil)
		return
	}
	if err := cli.DeleteConversation(b.ctx, cmd.ConversationID, cmd.Phone); err != nil {
		fail(cmd.ID, "request_failed", err)
		return
	}
	reply(cmd.ID, nil)
}

// archive moves a conversation to Google Messages' Archived (or, archive false, back to the inbox).
func (b *bridge) archive(cmd Command) {
	cli := b.current()
	if cli == nil {
		fail(cmd.ID, "not_connected", nil)
		return
	}
	status := gmproto.ConversationStatus_ACTIVE
	if cmd.Archive {
		status = gmproto.ConversationStatus_ARCHIVED
	}
	resp, err := cli.UpdateConversation(b.ctx, &gmproto.UpdateConversationRequest{
		ConversationID: cmd.ConversationID,
		Data: &gmproto.UpdateConversationRequest_UpdateData{UpdateData: &gmproto.UpdateConversationData{
			ConversationID: cmd.ConversationID,
			Data:           &gmproto.UpdateConversationData_Status{Status: status},
		}},
	})
	if err != nil {
		fail(cmd.ID, "request_failed", err)
		return
	}
	if !resp.GetSuccess() {
		fail(cmd.ID, "request_failed", errors.New("Google Messages did not archive it."))
		return
	}
	reply(cmd.ID, nil)
}

// simList lists the phone's SIM cards (for choosing one on dual-SIM phones).
func (b *bridge) simList(cmd Command) {
	b.mu.Lock()
	list := make([]SIM, 0, len(b.sims))
	for id, sim := range b.sims {
		list = append(list, SIM{ID: id, Carrier: sim.GetSIMData().GetCarrierName(), Number: sim.GetSIMData().GetFormattedPhoneNumber()})
	}
	b.mu.Unlock()
	reply(cmd.ID, map[string]any{"sims": list})
}

func (b *bridge) markRead(cmd Command) {
	cli := b.current()
	if cli == nil {
		fail(cmd.ID, "not_connected", nil)
		return
	}
	if err := cli.MarkRead(b.ctx, cmd.ConversationID, cmd.MessageID); err != nil {
		fail(cmd.ID, "request_failed", err)
		return
	}
	reply(cmd.ID, nil)
}

func (b *bridge) media(cmd Command) {
	cli := b.current()
	if cli == nil {
		fail(cmd.ID, "not_connected", nil)
		return
	}
	key, err := base64.StdEncoding.DecodeString(cmd.Key)
	if err != nil {
		fail(cmd.ID, "bad_key", err)
		return
	}
	body, err := cli.DownloadMedia(cmd.MediaID, key)
	if err != nil {
		fail(cmd.ID, "download_failed", err)
		return
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, 100<<20))
	if err != nil {
		fail(cmd.ID, "download_failed", err)
		return
	}
	reply(cmd.ID, map[string]any{"data": base64.StdEncoding.EncodeToString(data)})
}

func (b *bridge) unpair(cmd Command) {
	if cli := b.current(); cli != nil {
		if err := cli.Unpair(b.ctx); err != nil {
			b.log.Warn().Err(err).Msg("unpair failed")
		}
		cli.Disconnect()
	}
	b.mu.Lock()
	b.client = nil
	b.mu.Unlock()
	reply(cmd.ID, nil)
}

// ---------------------------------------------------------------- Conversion

func convertConversation(c *gmproto.Conversation) Conversation {
	people := make([]Participant, 0, len(c.GetParticipants()))
	for _, p := range c.GetParticipants() {
		people = append(people, Participant{
			ID:     p.GetID().GetParticipantID(),
			Name:   p.GetFullName(),
			Number: p.GetID().GetNumber(),
			IsMe:   p.GetIsMe() || p.GetID().GetParticipantID() == c.GetDefaultOutgoingID(),
		})
	}
	latest := c.GetLatestMessage()
	return Conversation{
		ID:            c.GetConversationID(),
		Name:          c.GetName(),
		IsRCS:         c.GetType() == gmproto.ConversationType_RCS,
		IsGroup:       c.GetIsGroupChat(),
		Unread:        c.GetUnread(),
		LastMs:        c.GetLastMessageTimestamp() / 1000,
		Snippet:       latest.GetDisplayContent(),
		SnippetFromMe: latest.GetFromMe() == 1,
		ReadOnly:      c.GetReadOnly(),
		Archived:      c.GetStatus() == gmproto.ConversationStatus_ARCHIVED || c.GetStatus() == gmproto.ConversationStatus_KEEP_ARCHIVED,
		DefaultSIM:    c.GetDefaultOutgoingID(),
		Participants:  people,
	}
}

func (b *bridge) convertMessage(m *gmproto.Message) Message {
	var text []string
	var media []Media
	for _, info := range m.GetMessageInfo() {
		if content := info.GetMessageContent(); content != nil && content.GetContent() != "" {
			text = append(text, content.GetContent())
		}
		if mc := info.GetMediaContent(); mc != nil {
			media = append(media, Media{
				MediaID: mc.GetMediaID(),
				Key:     base64.StdEncoding.EncodeToString(mc.GetDecryptionKey()),
				Mime:    mc.GetMimeType(),
				Name:    mc.GetMediaName(),
				Size:    mc.GetSize(),
			})
		}
	}
	b.mu.Lock()
	var reactions []Reaction
	for _, r := range m.GetReactions() {
		mine := false
		for _, id := range r.GetParticipantIDs() {
			if b.self[id] {
				mine = true
			}
		}
		if emoji := r.GetData().GetUnicode(); emoji != "" && len(r.GetParticipantIDs()) > 0 {
			reactions = append(reactions, Reaction{Emoji: emoji, Count: len(r.GetParticipantIDs()), Mine: mine})
		}
	}
	b.mu.Unlock()
	status := m.GetMessageStatus().GetStatus()
	return Message{
		Reactions:      reactions,
		ReplyTo:        m.GetReplyMessage().GetMessageID(),
		ID:             m.GetMessageID(),
		ConversationID: m.GetConversationID(),
		SenderName:     m.GetSenderParticipant().GetFullName(),
		FromMe:         status < 100,
		TimeMs:         m.GetTimestamp() / 1000,
		Text:           strings.Join(text, "\n"),
		Status:         statusName(status),
		Media:          media,
	}
}

func statusName(s gmproto.MessageStatusType) string {
	switch s {
	case gmproto.MessageStatusType_OUTGOING_DELIVERED:
		return "delivered"
	case gmproto.MessageStatusType_OUTGOING_DISPLAYED:
		return "read"
	case gmproto.MessageStatusType_OUTGOING_COMPLETE:
		return "sent"
	case gmproto.MessageStatusType_OUTGOING_SENDING, gmproto.MessageStatusType_OUTGOING_YET_TO_SEND,
		gmproto.MessageStatusType_OUTGOING_RESENDING, gmproto.MessageStatusType_OUTGOING_VALIDATING,
		gmproto.MessageStatusType_OUTGOING_SEND_AFTER_PROCESSING, gmproto.MessageStatusType_OUTGOING_AWAITING_RETRY:
		return "sending"
	}
	if s >= gmproto.MessageStatusType_OUTGOING_FAILED_GENERIC && s < 100 && s != gmproto.MessageStatusType_OUTGOING_DRAFT &&
		s != gmproto.MessageStatusType_OUTGOING_SCHEDULED && s != gmproto.MessageStatusType_OUTGOING_NOT_DELIVERED_YET {
		return "failed"
	}
	return ""
}

// ---------------------------------------------------------------- Main loop

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "-v") {
		fmt.Println("myphone-rcs-bridge", Version)
		return
	}
	// Warnings only, and never message content: libgm logs content only at trace level.
	log := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, NoColor: true}).Level(zerolog.WarnLevel).With().Timestamp().Logger()
	b := newBridge(log)
	scanner := bufio.NewScanner(os.Stdin)
	// Lines carry whole files (base64) for sendMedia: room for about 100 MB.
	scanner.Buffer(make([]byte, 1<<20), 140<<20)
	for scanner.Scan() {
		var cmd Command
		if err := json.Unmarshal(scanner.Bytes(), &cmd); err != nil {
			fail(0, "bad_command", err)
			continue
		}
		switch cmd.Cmd {
		case "start":
			b.start(cmd)
		case "startGoogle":
			b.pairGoogle(cmd)
		case "conversations":
			go b.conversations(cmd)
		case "messages":
			go b.messages(cmd)
		case "send":
			go b.sendText(cmd)
		case "sendMedia":
			go b.sendMedia(cmd)
		case "react":
			go b.react(cmd)
		case "setTyping":
			go b.setTyping(cmd)
		case "newChat":
			go b.newChat(cmd)
		case "deleteMessage":
			go b.deleteMessage(cmd)
		case "deleteConversation":
			go b.deleteConversation(cmd)
		case "archive":
			go b.archive(cmd)
		case "sims":
			b.simList(cmd)
		case "markRead":
			go b.markRead(cmd)
		case "media":
			go b.media(cmd)
		case "unpair":
			b.unpair(cmd)
		case "stop":
			reply(cmd.ID, nil)
			if cli := b.current(); cli != nil {
				cli.Disconnect()
			}
			b.cancel()
			return
		default:
			fail(cmd.ID, "unknown_command", nil)
		}
	}
	// My Phone closed: stop too.
	if cli := b.current(); cli != nil {
		cli.Disconnect()
	}
	b.cancel()
}
