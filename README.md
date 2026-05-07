# YouTube Live Dashboard

This version uses real Google OAuth and YouTube APIs through a local Node backend.

## Setup

1. Create a Google Cloud OAuth client:
   - Application type: Web application
   - Authorized redirect URI: `http://localhost:4173/oauth2callback`
   - Enable YouTube Data API v3
   - Enable YouTube Analytics API

2. Create `.env` from the template:

```bash
cp .env.example .env
```

3. Fill these values:

```bash
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
GOOGLE_REDIRECT_URI=http://localhost:4173/oauth2callback
YOUTUBE_API_KEY=...
ANTHROPIC_API_KEY=...
```

`YOUTUBE_API_KEY` is optional for public competitor data. `ANTHROPIC_API_KEY` is optional for future AI-written recommendations.

4. Start the app:

```bash
npm start
```

5. Open:

```text
http://localhost:4173
```

Do not open `index.html` directly with a `file://` URL. OAuth and live API calls only run through the local server.

## Using The Dashboard

- Click **Add channel** to login with Google and add/select a YouTube channel or Brand Account.
- The sidebar shows only connected channels, plus the Global overview.
- Click **Add competitor** to search public competitor channels by name, `@handle`, or channel ID.
- Competitors are attached to one of your connected channels.
- If YouTube blocks the `creatorContentType` analytics dimension for your account, the app estimates views split from video metadata/top content. Published counts still use uploaded video metadata.

## Notes

- Do not put API keys in `app.js`, `index.html`, or any browser file.
- Owned-channel analytics require Google login/OAuth.
- Competitor private analytics cannot be fetched unless that competitor also authorizes the app. Public competitor data uses YouTube Data API only.
