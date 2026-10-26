# thumbnails/ — Card Thumbnails

640x360 (16:9) WebP thumbnails for entity cards across the dashboard.

## Files

| File                        | Design                                                              | Used for         |
| --------------------------- | ------------------------------------------------------------------- | ---------------- |
| `dataset-thumbnail.webp`    | Slate card with centered table motif, blue header row, yellow corner chip | Dataset cards |
| `pipeline-thumbnail.webp`   | Slate flow of four pipeline nodes connected by arrows, final node highlighted yellow | Pipeline cards |

## Usage

```tsx
// Dataset card
<Image
  src="/assets/images/thumbnails/dataset-thumbnail.webp"
  alt="Dataset"
  width={640}
  height={360}
  className="rounded-xl"
/>
```

## Guidelines

- Render at 2x display size maximum (they are 640px wide; ideal for cards up
  to 320 CSS px).
- These are decorative defaults; datasets or pipelines may override with
  user-supplied cover images at the same 16:9 ratio.
- Regenerate via `scripts/generate_webp_assets.py` after palette changes.
