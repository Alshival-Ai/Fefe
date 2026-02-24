(function () {
  const storageKey = 'fefe-theme';
  const root = document.documentElement;
  const toggles = document.querySelectorAll('.theme__toggle');
  const prefersDark = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;

  const applyTheme = (mode) => {
    root.classList.toggle('light-style', mode === 'light');
  };

  const stored = localStorage.getItem(storageKey);
  const initial = stored || (prefersDark && prefersDark.matches ? 'dark' : 'light');
  applyTheme(initial);

  toggles.forEach((toggle) => {
    toggle.checked = initial === 'dark';
    toggle.addEventListener('change', () => {
      const next = toggle.checked ? 'dark' : 'light';
      applyTheme(next);
      localStorage.setItem(storageKey, next);
      toggles.forEach((other) => {
        if (other !== toggle) {
          other.checked = toggle.checked;
        }
      });
    });
  });

  if (prefersDark && !stored) {
    const handleChange = (event) => {
      const mode = event.matches ? 'dark' : 'light';
      applyTheme(mode);
      toggles.forEach((toggle) => {
        toggle.checked = mode === 'dark';
      });
    };
    if (typeof prefersDark.addEventListener === 'function') {
      prefersDark.addEventListener('change', handleChange);
    } else if (typeof prefersDark.addListener === 'function') {
      prefersDark.addListener(handleChange);
    }
  }
})();
