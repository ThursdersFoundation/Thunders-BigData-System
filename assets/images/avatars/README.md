# Thunders BigData System — Default Avatar

`default-avatar.svg` is the fallback avatar shown when a user, workspace, or
integration has no profile image. It is a Thunder Blue (#2563EB) circle with
a white head and shoulders, clipped to the circular frame by an internal
clipPath, so it stays perfectly round at any size.

## File

| File | Source size | Usage |
| --- | --- | --- |
| default-avatar.svg | 96x96 | Fallback avatar wherever a profile image is missing |

## Usage

- Use it as the default `src` for avatar components and as the `onError`
  fallback when a remote profile photo fails to load.
- Render at 96 px (profile pages), 48 px (lists and comments), or 32 px
  (compact table rows). The 96x96 source scales cleanly to all three.
- Keep width and height equal; the artwork is designed for a 1:1 circle.
- Alt text suggestion: "User avatar", or the user's name when known.

## Don't

- Don't recolor the background per user; the avatar is brand-fixed.
- Don't add external borders or shadows that break the circular silhouette.
