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
    initMatrixCanvas(pageMatrixCanvas, 'element');
  }
})();
