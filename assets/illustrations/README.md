# Thunders BigData System — Empty-State Illustrations

Flat, friendly SVG illustrations for empty and system states. All files share
a 320x240 viewBox, a transparent background, the brand palette only, and no
text elements (headings and copy belong to the UI layer).

## Files

| File | Intended usage |
| --- | --- |
| empty-dataset.svg | Empty dataset / query results page |
| empty-pipeline.svg | Empty pipeline list, no active jobs |
| no-data.svg | Generic no-data placeholder |
| error-state.svg | Error banners and failure states |
| maintenance.svg | Maintenance mode or scheduled downtime page |

## Style rules

- Canvas is fixed at viewBox="0 0 320 240"; scale via width/height only.
- Flat shapes, rounded corners, brand palette only (Slate #0F172A,
  Thunder Blue #2563EB, Bolt Yellow #FACC15, grays #94A3B8 / #E2E8F0).
- No gradients, photos, or embedded text; each SVG ships title/desc for
  direct <img> use, and UI copy stays in the embedding component.

## Usage (React / Next.js)

```tsx
import EmptyDataset from "@/assets/images/illustrations/empty-dataset.svg";

export function EmptyState() {
  return (
    <EmptyDataset width={320} height={240} role="img"
      aria-label="No records found" />
  );
}
```

Plain HTML: `<img src="/assets/images/illustrations/empty-dataset.svg" alt="No records found" width="320" height="240" />`
