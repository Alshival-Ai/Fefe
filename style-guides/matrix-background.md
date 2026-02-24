# Matrix Background (Exact Replica)

This document describes the exact Matrix-style background used on the landing page so another agent can reproduce it precisely.

**Source of truth in this repo**
- `apps/main/templates/index.html` (CSS variables + canvas element)
- `src/assets/js/front-page-landing.js` (canvas drawing logic)

---

## Visual Behavior Summary
- Full-viewport fixed canvas that renders falling characters in two alternating colors.
- A translucent fade overlay is drawn each frame to create trailing streaks.
- Character glyphs are monospaced at 16px, columns aligned to font size.
- Animation runs at ~20 FPS (`frameDelay = 50ms`) via `requestAnimationFrame`.
- Uses system `monospace` font.
- Respects `prefers-reduced-motion`: **no animation** if user prefers reduced motion.
- Color palette is theme-aware via CSS variables on `:root` and `.light-style`.

---

## HTML (Canvas Element)
Place this near the top of the page body (behind other content):

```html
<canvas id="landing-page-matrix" class="landing-page-matrix" aria-hidden="true"></canvas>
```

---

## CSS (Exact Styles and Variables)
These styles are defined in `apps/main/templates/index.html`. Replicate them exactly to keep the look consistent.

```css
:root {
  --matrix-color-a: #5eead4;
  --matrix-color-b: #38bdf8;
  --matrix-fade: rgba(7, 10, 20, 0.15);
  --matrix-button-ink: #0b1324;
  --matrix-button-border: rgba(255, 255, 255, 0.35);
  --matrix-button-border-hover: rgba(255, 255, 255, 0.6);
  --matrix-button-glow: rgba(56, 189, 248, 0.45);
}

.light-style {
  --matrix-color-a: #0f766e;
  --matrix-color-b: #1d4ed8;
  --matrix-fade: rgba(248, 250, 252, 0.1);
  --matrix-button-ink: #f8fafc;
  --matrix-button-border: rgba(15, 23, 42, 0.2);
  --matrix-button-border-hover: rgba(15, 23, 42, 0.35);
  --matrix-button-glow: rgba(29, 78, 216, 0.35);
}

.landing-page-matrix {
  position: fixed;
  inset: 0;
  z-index: 0;
  width: 100%;
  height: 100%;
  display: block;
  opacity: 0.35;
  pointer-events: none;
}

.light-style .landing-page-matrix {
  opacity: 0.6;
}

.landing-page-content {
  position: relative;
  z-index: 1;
}
```

Notes:
- The canvas is full-viewport, fixed, and non-interactive.
- Content above it should sit on a higher `z-index`.

---

## JavaScript (Exact Rendering Logic)
This is the exact logic from `src/assets/js/front-page-landing.js` distilled to the matrix background only. Use it as-is for exact reproduction.

```js
(function () {
  const pageMatrixCanvas = document.getElementById('landing-page-matrix');

  const initMatrixCanvas = function (canvas, sizeMode) {
    const prefersReducedMotion = window.matchMedia
      ? window.matchMedia('(prefers-reduced-motion: reduce)').matches
      : false;
    if (prefersReducedMotion) {
      return;
    }

    const ctx = canvas.getContext('2d');
    if (!ctx) {
      return;
    }

    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@#$%&*+-=∑ΣπθλΩαβγΔλμ∀∂∫√∞≈→←あカサタナハマヤラワンアイウエオ한글테스트';
    const fontSize = 16;
    let columns = 0;
    let drops = [];
    let animationFrame = null;
    let resizeTimeout = null;
    let lastFrame = 0;
    let lastWidth = 0;
    let lastHeight = 0;
    const frameDelay = 50;
    let colorA = '#5eead4';
    let colorB = '#38bdf8';
    let fadeColor = 'rgba(7, 10, 20, 0.15)';

    const applyThemeColors = function () {
      const styles = window.getComputedStyle(canvas);
      const nextColorA = styles.getPropertyValue('--matrix-color-a').trim();
      const nextColorB = styles.getPropertyValue('--matrix-color-b').trim();
      const nextFade = styles.getPropertyValue('--matrix-fade').trim();
      if (nextColorA) colorA = nextColorA;
      if (nextColorB) colorB = nextColorB;
      if (nextFade) fadeColor = nextFade;
    };

    const getCanvasRect = function () {
      if (sizeMode === 'viewport') {
        return {
          width: window.innerWidth,
          height: window.innerHeight
        };
      }
      return canvas.getBoundingClientRect();
    };

    const setCanvasSize = function () {
      const dpr = window.devicePixelRatio || 1;
      const rect = getCanvasRect();
      const widthChanged = !lastWidth || Math.round(rect.width) !== Math.round(lastWidth);
      const heightChanged = !lastHeight || Math.round(rect.height) !== Math.round(lastHeight);

      if (sizeMode === 'viewport' && !widthChanged) {
        return;
      }

      if (!widthChanged && !heightChanged) {
        return;
      }

      lastWidth = rect.width;
      lastHeight = rect.height;
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

      if (widthChanged) {
        columns = Math.max(1, Math.floor(rect.width / fontSize));
        drops = new Array(columns).fill(0).map(function () {
          return Math.random() * rect.height;
        });
      }
    };

    const draw = function (timestamp) {
      if (timestamp - lastFrame < frameDelay) {
        animationFrame = window.requestAnimationFrame(draw);
        return;
      }
      lastFrame = timestamp;

      ctx.fillStyle = fadeColor;
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.font = fontSize + 'px monospace';

      for (let i = 0; i < drops.length; i += 1) {
        const text = characters.charAt(Math.floor(Math.random() * characters.length));
        const x = i * fontSize;
        const y = drops[i] * fontSize;
        ctx.fillStyle = i % 3 === 0 ? colorA : colorB;
        ctx.fillText(text, x, y);
        if (y > canvas.height / (window.devicePixelRatio || 1) && Math.random() > 0.95) {
          drops[i] = 0;
        } else {
          drops[i] += 1;
        }
      }

      animationFrame = window.requestAnimationFrame(draw);
    };

    const handleResize = function () {
      window.clearTimeout(resizeTimeout);
      resizeTimeout = window.setTimeout(setCanvasSize, 120);
    };

    applyThemeColors();
    setCanvasSize();
    window.addEventListener('resize', handleResize);
    animationFrame = window.requestAnimationFrame(draw);

    const themeObserver = new MutationObserver(function (mutations) {
      if (mutations.some(function (mutation) {
        return mutation.attributeName === 'class';
      })) {
        applyThemeColors();
      }
    });

    themeObserver.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class']
    });

    window.addEventListener('beforeunload', function () {
      if (animationFrame) {
        window.cancelAnimationFrame(animationFrame);
      }
      window.removeEventListener('resize', handleResize);
      themeObserver.disconnect();
    });
  };

  if (pageMatrixCanvas) {
    initMatrixCanvas(pageMatrixCanvas, 'viewport');
  }
})();
```

---

## Required Colors and Opacity (Exact)
Dark/default theme (`:root`):
- `--matrix-color-a`: `#5eead4`
- `--matrix-color-b`: `#38bdf8`
- `--matrix-fade`: `rgba(7, 10, 20, 0.15)`
- Canvas opacity: `0.35`

Light theme (`.light-style`):
- `--matrix-color-a`: `#0f766e`
- `--matrix-color-b`: `#1d4ed8`
- `--matrix-fade`: `rgba(248, 250, 252, 0.1)`
- Canvas opacity: `0.6`

---

## Required Character Set (Exact)
The renderer picks random characters from this exact string (order matters):

```
ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@#$%&*+-=∑ΣπθλΩαβγΔλμ∀∂∫√∞≈→←あカサタナハマヤラワンアイウエオ한글테스트
```

---

## Timing and Sizing (Exact)
- `fontSize`: `16`
- `frameDelay`: `50` (milliseconds)
- Resize debounce: `120` (milliseconds)
- Columns: `Math.floor(width / fontSize)`
- The canvas scales to device pixel ratio (`devicePixelRatio`) and uses `ctx.setTransform(dpr, 0, 0, dpr, 0, 0)`.

---

## Integration Checklist
1. Add the canvas element and CSS.
2. Include the JS block exactly as written.
3. Ensure the page toggles `.light-style` on the root `html` element to switch theme colors if needed.
4. Keep the canvas fixed with `opacity` values above for exact visual match.
5. Confirm `prefers-reduced-motion` is respected (no animation when enabled).

---

## Optional Notes (Parity with Current Site)
- The current landing page uses `z-index: 1` on `.landing-page-content` so the canvas stays behind all content.
- The matrix effect is independent of the rest of `front-page-landing.js` and can be extracted standalone.
