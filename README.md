# myphone-rcs-bridge

A small helper program that lets [My Phone](https://github.com/JustCallMeAD) (a Windows app that links a PC to an Android phone) read and answer **Google Messages** conversations, RCS chats included, the same way Google Messages for web does.

My Phone starts the helper only when its "Google Messages on this PC" switch is on (it is off by default). The helper pairs with Google Messages through a QR code that you scan on the phone (Google Messages, Device pairing), then passes conversations and messages to My Phone.

## Unofficial

This uses the same connection as messages.google.com, through [libgm](https://github.com/mautrix/gmessages/tree/main/pkg/libgm) from mautrix-gmessages. It is not an official Google interface: Google can change or block it at any time, and only one web session is active at a time (opening Google Messages for web elsewhere pauses this one).

## How My Phone talks to it

One JSON object per line on standard input (commands) and standard output (replies and events). Logs go to standard error, at warning level, and never include message text, names or numbers.

Commands (each with a numeric `id`, answered by `{"id":…, "ok":true|false, …}`):

| `cmd` | Fields | Reply |
| --- | --- | --- |
| `start` | `session` (the JSON from a `session` event, or `null` to pair) | `pairing`: true when a QR code follows |
| `startGoogle` | `cookies` (Google sign-in cookies: SID, HSID, SSID, APISID, SAPISID, OSID, __Secure-1PSIDTS) | `pairing`: true; an `emoji` event follows, to tap on the phone |
| `conversations` | `limit` | `conversations`: list |
| `messages` | `conversationId`, `limit` | `messages`: list, oldest last |
| `send` | `conversationId`, `text` | |
| `markRead` | `conversationId`, `messageId` | |
| `media` | `mediaId`, `key` (base64) | `data` (base64) |
| `unpair` | | |
| `stop` | | |

Events: `qr` (`url` to show as a QR code), `emoji` (`emoji` to tap on the phone when pairing through the Google account), `paired`, `pairFailed`, `session` (keep it, encrypted, to `start` again later), `connected`, `ready`, `conversation`, `message`, `state`, `disconnected`, `unpaired`.

## Building

```
go build -trimpath -ldflags "-s -w" -o myphone-rcs-bridge.exe .
```

## Licence

GNU Affero General Public License v3.0 or later, the same as mautrix-gmessages, whose libgm this program uses. See [LICENSE](LICENSE).
