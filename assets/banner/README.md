# banners/ — Page Banners

Wide WebP banners for page headers and marketing surfaces.

## Files

| File                     | Size      | Design                                                       |
| ------------------------ | --------- | ------------------------------------------------------------ |
| `dashboard-banner.webp`  | 1200x400  | Slate-to-blue horizontal gradient, diagonal bolt-yellow accent stripes, translucent bolt watermark |

## Usage

```tsx
// Page hero / header banner
import banner from "@/assets/images/banners/dashboard-banner.webp";

<section
  className="rounded-2xl bg-cover bg-center"
  style={{ backgroundImage: `url(${banner.src})` }}
>
  <h1>Operations Overview</h1>
</section>
```

## Guidelines

- The banner is designed for 3:1 aspect ratios; crop with `background-position`
  rather than stretching when the container ratio differs.
- Light text (`#FFFFFF`, `#E2E8F0`) is safe across the full width; the blue
  right side tolerates Bolt Yellow badges for CTAs.
- Regenerate via `scripts/generate_webp_assets.py` when brand colors change.
