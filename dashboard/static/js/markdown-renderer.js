(function () {
  const escapeHtml = (value) => String(value || '').replace(/[&<>"']/g, (char) => {
    if (char === '&') return '&amp;';
    if (char === '<') return '&lt;';
    if (char === '>') return '&gt;';
    if (char === '"') return '&quot;';
    return '&#39;';
  });

  const fallbackHtml = (markdownText) => {
    const source = String(markdownText || '').trim();
    if (!source) return '';
    return `<p>${escapeHtml(source).replace(/\n/g, '<br>')}</p>`;
  };

  const getRoot = () => document.body || document.documentElement;
  const getDataValue = (name) => {
    const root = getRoot();
    return String((root && root.getAttribute(name)) || '').trim();
  };

  const ensureScript = (src) => new Promise((resolve, reject) => {
    const resolved = String(src || '').trim();
    if (!resolved) {
      reject(new Error('missing_src'));
      return;
    }
    const existing = Array.from(document.querySelectorAll('script')).find(
      (node) => String(node.getAttribute('src') || '').trim() === resolved
        || String(node.getAttribute('data-alshival-markdown-src') || '').trim() === resolved
    );
    if (existing) {
      if (existing.dataset && existing.dataset.alshivalMarkdownLoaded === '1') {
        resolve();
        return;
      }
      existing.addEventListener('load', () => resolve(), { once: true });
      existing.addEventListener('error', () => reject(new Error(`Failed to load ${resolved}`)), { once: true });
      return;
    }
    const script = document.createElement('script');
    script.src = resolved;
    script.async = true;
    script.setAttribute('data-alshival-markdown-src', resolved);
    script.onload = () => {
      script.dataset.alshivalMarkdownLoaded = '1';
      resolve();
    };
    script.onerror = () => reject(new Error(`Failed to load ${resolved}`));
    document.head.appendChild(script);
  });

  let depsPromise = null;
  const ensureDeps = () => {
    if (window.marked && typeof window.marked.parse === 'function' && window.DOMPurify && typeof window.DOMPurify.sanitize === 'function') {
      return Promise.resolve(true);
    }
    if (depsPromise) return depsPromise;

    const markdownSrc = getDataValue('data-markdown-script-src');
    const sanitizerSrc = getDataValue('data-sanitizer-script-src');
    const loaders = [];
    if (!(window.marked && typeof window.marked.parse === 'function') && markdownSrc) {
      loaders.push(ensureScript(markdownSrc));
    }
    if (!(window.DOMPurify && typeof window.DOMPurify.sanitize === 'function') && sanitizerSrc) {
      loaders.push(ensureScript(sanitizerSrc));
    }
    if (!loaders.length) return Promise.resolve(false);

    depsPromise = Promise.all(loaders).then(() => true).catch(() => false);
    return depsPromise;
  };

  const sanitizeHtml = (unsafeHtml) => {
    const html = String(unsafeHtml || '').trim();
    if (!html) return '';
    if (!window.DOMPurify || typeof window.DOMPurify.sanitize !== 'function') return html;
    try {
      return String(
        window.DOMPurify.sanitize(html, {
          USE_PROFILES: { html: true },
          ADD_ATTR: ['target', 'rel', 'class', 'id', 'name', 'align'],
          FORBID_TAGS: ['script', 'style'],
          ALLOW_DATA_ATTR: false,
        }) || ''
      ).trim();
    } catch (error) {
      return html;
    }
  };

  const normalizeLinks = (root) => {
    if (!root) return;
    root.querySelectorAll('a[href]').forEach((anchor) => {
      const href = String(anchor.getAttribute('href') || '').trim().toLowerCase();
      const isExternal = href
        && !href.startsWith('#')
        && !href.startsWith('/')
        && !href.startsWith('mailto:')
        && !href.startsWith('tel:');
      if (!isExternal) return;
      anchor.setAttribute('target', '_blank');
      anchor.setAttribute('rel', 'noopener noreferrer');
    });
  };

  const renderToHtml = async (markdownText, options = {}) => {
    const source = String(markdownText || '').trim();
    const emptyHtml = String((options && options.emptyHtml) || '').trim();
    if (!source) return emptyHtml;

    await ensureDeps();
    let rendered = '';
    if (window.marked && typeof window.marked.parse === 'function') {
      try {
        const breaks = Object.prototype.hasOwnProperty.call(options || {}, 'breaks')
          ? Boolean(options.breaks)
          : true;
        rendered = String(
          window.marked.parse(source, {
            gfm: true,
            breaks,
            mangle: false,
          }) || ''
        ).trim();
      } catch (error) {
        rendered = '';
      }
    }
    const html = sanitizeHtml(rendered || fallbackHtml(source));
    return html || fallbackHtml(source);
  };

  const renderInto = async (target, markdownText, options = {}) => {
    if (!target) return '';
    const html = await renderToHtml(markdownText, options);
    target.innerHTML = html;
    normalizeLinks(target);
    return html;
  };

  window.AlshivalMarkdown = {
    ensureDeps,
    fallbackHtml,
    renderToHtml,
    renderInto,
  };
})();
