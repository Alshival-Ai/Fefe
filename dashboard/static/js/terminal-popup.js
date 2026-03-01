(function () {
  const DEFAULT_WS_PATH = '/terminal/ws/';
  const DEFAULT_HINT = 'Ctrl/Cmd+C copy selection, Ctrl/Cmd+click open links, Ctrl+Shift+V paste, Ctrl+/- zoom';
  const DEFAULT_FEATURES = 'width=1100,height=760,resizable=yes,scrollbars=no';
  const ASK_POPOUT_FEATURES = 'popup=yes,width=520,height=760,resizable=yes,scrollbars=yes';
  const FONT_PREF_KEY = 'devtools_terminal_font_size';
  const ASK_CHAT_ENDPOINT = '/chat/ask/';
  const ASK_CHAT_HISTORY_ENDPOINT = '/chat/history/';
  const ASK_CHAT_HISTORY_CLEAR_ENDPOINT = '/chat/history/clear/';
  const ASK_SESSION_GREETING_ENDPOINT = '/chat/session-greeting/';
  const ASK_VOICE_TOKEN_ENDPOINT = '/chat/voice-token/';
  const ASK_VOICE_LOG_ENDPOINT = '/chat/voice-log/';
  const ASK_POPOUT_PATH = '/chat/widget/';
  const AGENT_BUBBLE_DEFAULT_MS = 5000;
  const AGENT_BUBBLE_MIN_MS = 1200;
  const AGENT_BUBBLE_MAX_MS = 60000;

  let xtermLoaderPromise = null;
  let askWidget = null;
  let askClient = null;
  let askWidgetDragCleanup = null;
  let agentBubble = null;
  let agentBubbleContent = null;
  let agentBubbleHideTimer = null;
  let agentBubbleRenderToken = 0;
  const embeddedAskMounts = new WeakMap();
  const root = document.body || document.documentElement;
  const isStaff = String(
    (root && (root.getAttribute('data-staff') || root.getAttribute('data-superuser'))) || '0'
  ) === '1';

  const escapeHtml = (value) => String(value || '').replace(/[&<>"']/g, (char) => {
    if (char === '&') return '&amp;';
    if (char === '<') return '&lt;';
    if (char === '>') return '&gt;';
    if (char === '"') return '&quot;';
    return '&#39;';
  });

  const renderMarkdownInto = async (target, markdownText, options = {}) => {
    if (!target) return '';
    const source = String(markdownText || '').trim();
    if (!source) {
      target.innerHTML = '';
      return '';
    }
    const markdown = window.AlshivalMarkdown;
    if (markdown && typeof markdown.renderInto === 'function') {
      try {
        return await markdown.renderInto(target, source, options);
      } catch (error) {}
    }
    target.innerHTML = `<p>${escapeHtml(source).replace(/\n/g, '<br>')}</p>`;
    return target.innerHTML;
  };

  const getCookie = (name) => {
    const value = `; ${document.cookie || ''}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length !== 2) return '';
    return parts.pop().split(';').shift() || '';
  };

  const ensureStyle = (href) => {
    if (document.querySelector(`link[data-terminal-popup-css="${href}"]`)) return;
    const link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = href;
    link.setAttribute('data-terminal-popup-css', href);
    document.head.appendChild(link);
  };

  const ensureScript = (src) => new Promise((resolve, reject) => {
    if (document.querySelector(`script[data-terminal-popup-js="${src}"]`)) {
      resolve();
      return;
    }
    const script = document.createElement('script');
    script.src = src;
    script.async = true;
    script.setAttribute('data-terminal-popup-js', src);
    script.onload = () => resolve();
    script.onerror = () => reject(new Error(`Failed to load ${src}`));
    document.head.appendChild(script);
  });

  const parseDurationMs = (value, fallback) => {
    const raw = Number(value);
    if (!Number.isFinite(raw)) return fallback;
    return Math.max(AGENT_BUBBLE_MIN_MS, Math.min(AGENT_BUBBLE_MAX_MS, Math.round(raw)));
  };

  const ensureAgentBubbleElement = () => {
    if (agentBubble && agentBubbleContent) return;
    agentBubble = document.createElement('aside');
    agentBubble.className = 'floating-ask-agent-bubble';
    agentBubble.setAttribute('aria-live', 'polite');
    agentBubble.setAttribute('aria-atomic', 'true');
    agentBubble.hidden = true;
    agentBubble.innerHTML = `
      <button type="button" class="floating-ask-agent-bubble__close" aria-label="Dismiss message">×</button>
      <div class="floating-ask-agent-bubble__content"></div>
    `;
    document.body.appendChild(agentBubble);
    agentBubbleContent = agentBubble.querySelector('.floating-ask-agent-bubble__content');
    const closeButton = agentBubble.querySelector('.floating-ask-agent-bubble__close');
    if (closeButton) {
      closeButton.addEventListener('click', () => {
        if (agentBubbleHideTimer) {
          window.clearTimeout(agentBubbleHideTimer);
          agentBubbleHideTimer = null;
        }
        if (agentBubble) {
          agentBubble.classList.remove('is-visible');
          window.setTimeout(() => {
            if (!agentBubble || agentBubble.classList.contains('is-visible')) return;
            agentBubble.hidden = true;
          }, 180);
        }
      });
    }
  };

  const hideAgentBubble = () => {
    if (agentBubbleHideTimer) {
      window.clearTimeout(agentBubbleHideTimer);
      agentBubbleHideTimer = null;
    }
    if (!agentBubble) return;
    agentBubble.classList.remove('is-visible');
    window.setTimeout(() => {
      if (!agentBubble || agentBubble.classList.contains('is-visible')) return;
      agentBubble.hidden = true;
    }, 180);
  };

  const showAgentBubble = async (askButton, markdownText, options = {}) => {
    if (!askButton) return false;
    const message = String(markdownText || '').trim();
    if (!message) return false;
    ensureAgentBubbleElement();
    if (!agentBubble || !agentBubbleContent) return false;

    const token = ++agentBubbleRenderToken;
    const defaultDurationMs = parseDurationMs(
      String((askButton.dataset && askButton.dataset.agentBubbleDurationMs) || ''),
      AGENT_BUBBLE_DEFAULT_MS
    );
    const hasCustomDuration = options && Object.prototype.hasOwnProperty.call(options, 'durationMs');
    const durationMs = hasCustomDuration
      ? parseDurationMs(options.durationMs, defaultDurationMs)
      : defaultDurationMs;
    const persist = Boolean(options && options.persist);

    if (token !== agentBubbleRenderToken || !agentBubbleContent || !agentBubble) return false;
    await renderMarkdownInto(agentBubbleContent, message, { breaks: true });
    if (token !== agentBubbleRenderToken || !agentBubbleContent || !agentBubble) return false;
    agentBubbleContent.querySelectorAll('a').forEach((link) => {
      link.setAttribute('target', '_blank');
      link.setAttribute('rel', 'noopener noreferrer');
    });

    if (agentBubbleHideTimer) {
      window.clearTimeout(agentBubbleHideTimer);
      agentBubbleHideTimer = null;
    }
    agentBubble.hidden = false;
    requestAnimationFrame(() => {
      if (!agentBubble) return;
      agentBubble.classList.add('is-visible');
    });
    if (!persist) {
      agentBubbleHideTimer = window.setTimeout(() => {
        hideAgentBubble();
      }, durationMs);
    }
    return true;
  };

  const maybeShowSessionGreeting = async (askButton) => {
    if (!askButton) return;
    const currentPath = String((window.location && window.location.pathname) || '').trim();
    if (currentPath === ASK_POPOUT_PATH) return;
    if (document.body && document.body.classList.contains('ask-widget-popout-body')) return;
    try {
      const response = await fetch(ASK_SESSION_GREETING_ENDPOINT, {
        method: 'GET',
        credentials: 'same-origin',
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload || payload.ok !== true) return;
      if (!payload.show) return;
      const markdown = String(payload.markdown || '').trim();
      if (!markdown) return;
      const durationMs = parseDurationMs(payload.duration_ms, AGENT_BUBBLE_DEFAULT_MS);
      await showAgentBubble(askButton, markdown, { durationMs });
    } catch (error) {}
  };

  const ensureXterm = () => {
    if (window.Terminal && window.FitAddon && window.FitAddon.FitAddon) {
      return Promise.resolve();
    }
    if (xtermLoaderPromise) return xtermLoaderPromise;

    ensureStyle('https://cdn.jsdelivr.net/npm/xterm/css/xterm.css');
    xtermLoaderPromise = Promise.all([
      ensureScript('https://cdn.jsdelivr.net/npm/xterm/lib/xterm.js'),
      ensureScript('https://cdn.jsdelivr.net/npm/xterm-addon-fit/lib/xterm-addon-fit.js'),
      ensureScript('https://cdn.jsdelivr.net/npm/xterm-addon-web-links/lib/xterm-addon-web-links.js'),
    ]).then(() => undefined);
    return xtermLoaderPromise;
  };

  const clampFontSize = (value) => Math.max(11, Math.min(22, value));

  const loadFontSize = () => {
    try {
      const raw = Number(window.localStorage.getItem(FONT_PREF_KEY));
      if (Number.isFinite(raw)) return clampFontSize(raw);
    } catch (err) {}
    return 14;
  };

  const saveFontSize = (value) => {
    try {
      window.localStorage.setItem(FONT_PREF_KEY, String(clampFontSize(value)));
    } catch (err) {}
  };

  const buildWsPath = (wsPath, query) => {
    const base = String(wsPath || DEFAULT_WS_PATH);
    const params = new URLSearchParams(query || {});
    const q = params.toString();
    if (!q) return base;
    return base + (base.indexOf('?') === -1 ? '?' : '&') + q;
  };

  const shouldActivateTerminalLink = (event) => Boolean(event && (event.ctrlKey || event.metaKey));

  const openTerminalLink = (scopeWindow, uri) => {
    if (!uri) return;
    const activeWindow = scopeWindow && typeof scopeWindow.open === 'function' ? scopeWindow : window;
    const nextWindow = activeWindow.open();
    if (nextWindow) {
      try { nextWindow.opener = null; } catch (err) {}
      nextWindow.location.href = uri;
      return;
    }
    console.warn('Opening link blocked as opener could not be cleared');
  };

  const makeTerminalClient = async ({ container, title, hintText, wsPath, onSocketClose }) => {
    await ensureXterm();

    const wrap = document.createElement('div');
    wrap.className = 'terminal-inline-wrap';
    wrap.innerHTML = `
      <div class="terminal-inline-bar">
        <span class="terminal-inline-meta">
          <span class="terminal-inline-title">${escapeHtml(title)}</span>
          <span class="terminal-inline-hint">${escapeHtml(hintText || DEFAULT_HINT)}</span>
        </span>
        <span class="terminal-inline-status" aria-live="polite"></span>
      </div>
      <div class="terminal-inline-body"></div>
    `;
    container.innerHTML = '';
    container.appendChild(wrap);

    const termEl = wrap.querySelector('.terminal-inline-body');
    const statusEl = wrap.querySelector('.terminal-inline-status');

    const term = new window.Terminal({
      cursorBlink: true,
      cursorStyle: 'block',
      cursorInactiveStyle: 'outline',
      scrollback: 50000,
      fontFamily: '"SFMono-Regular","Menlo","Monaco","Consolas","Liberation Mono","Courier New",monospace',
      fontSize: loadFontSize(),
      theme: { background: '#0a0e12', foreground: '#d7e2f2', cursor: '#6be4a8', selection: 'rgba(107, 228, 168, 0.3)' },
    });

    const fit = new window.FitAddon.FitAddon();
    term.loadAddon(fit);
    if (window.WebLinksAddon && window.WebLinksAddon.WebLinksAddon) {
      term.loadAddon(new window.WebLinksAddon.WebLinksAddon((event, uri) => {
        if (!shouldActivateTerminalLink(event)) return;
        openTerminalLink(window, uri);
      }));
    }
    term.open(termEl);
    fit.fit();

    const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(scheme + '://' + window.location.host + wsPath);
    ws.binaryType = 'arraybuffer';
    const socketStartedAt = Date.now();
    let socketOpened = false;

    let statusTimer = null;
    let lastCopiedSelection = '';
    let currentFontSize = Number(term.options.fontSize) || 14;

    const setStatus = (message, isError) => {
      if (!statusEl) return;
      statusEl.textContent = message || '';
      statusEl.classList.toggle('error', Boolean(isError));
      if (statusTimer) window.clearTimeout(statusTimer);
      if (!message) return;
      statusTimer = window.setTimeout(() => {
        statusEl.textContent = '';
        statusEl.classList.remove('error');
      }, isError ? 2500 : 1200);
    };

    const copySelection = async () => {
      const selected = term.getSelection ? term.getSelection() : '';
      if (!selected) return false;
      if (selected === lastCopiedSelection) return true;
      try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
          await navigator.clipboard.writeText(selected);
          lastCopiedSelection = selected;
          setStatus('Copied', false);
          return true;
        }
      } catch (err) {}
      return false;
    };

    const pasteFromClipboard = async () => {
      if (!(navigator.clipboard && navigator.clipboard.readText)) {
        setStatus('Clipboard read unavailable', true);
        return false;
      }
      try {
        const text = await navigator.clipboard.readText();
        if (!text) {
          setStatus('Clipboard empty', true);
          return false;
        }
        if (ws.readyState !== WebSocket.OPEN) {
          setStatus('Disconnected', true);
          return false;
        }
        ws.send(text);
        setStatus('Pasted', false);
        return true;
      } catch (err) {
        setStatus('Clipboard blocked', true);
        return false;
      }
    };

    const sendResize = () => {
      const dims = fit.proposeDimensions();
      if (!dims || ws.readyState !== WebSocket.OPEN) return;
      ws.send(JSON.stringify({ type: 'resize', cols: dims.cols, rows: dims.rows }));
    };

    const setFontSize = (nextSize) => {
      const fontSize = clampFontSize(nextSize);
      if (fontSize === currentFontSize) return;
      currentFontSize = fontSize;
      term.options.fontSize = fontSize;
      saveFontSize(fontSize);
      fit.fit();
      sendResize();
      setStatus(`Font ${String(fontSize)}px`, false);
    };

    ws.addEventListener('open', () => {
      socketOpened = true;
      term.focus();
      sendResize();
      setStatus('Connected', false);
    });

    ws.addEventListener('message', (event) => {
      if (event.data instanceof ArrayBuffer) {
        term.write(new Uint8Array(event.data));
        return;
      }
      term.write(event.data);
    });

    ws.addEventListener('close', () => {
      term.write('\r\n[terminal session closed]\r\n');
      setStatus('Disconnected', true);
      if (typeof onSocketClose === 'function') {
        onSocketClose({
          opened: socketOpened,
          durationMs: Date.now() - socketStartedAt,
        });
      }
    });

    term.attachCustomKeyEventHandler((event) => {
      const key = (event.key || '').toLowerCase();
      const ctrlOrMeta = Boolean(event.ctrlKey || event.metaKey);
      const hasSelection = Boolean(term.hasSelection && term.hasSelection());
      const wantsCopy = hasSelection && ((ctrlOrMeta && key === 'c') || (event.ctrlKey && event.shiftKey && key === 'c'));
      if (wantsCopy) {
        event.preventDefault();
        copySelection();
        return false;
      }
      const wantsPaste = (event.ctrlKey && event.shiftKey && key === 'v') || (event.metaKey && key === 'v') || (event.shiftKey && key === 'insert');
      if (wantsPaste) {
        event.preventDefault();
        pasteFromClipboard();
        return false;
      }
      if (event.ctrlKey && (key === '=' || key === '+')) {
        event.preventDefault();
        setFontSize(currentFontSize + 1);
        return false;
      }
      if (event.ctrlKey && key === '-') {
        event.preventDefault();
        setFontSize(currentFontSize - 1);
        return false;
      }
      if (event.ctrlKey && key === '0') {
        event.preventDefault();
        setFontSize(14);
        return false;
      }
      return true;
    });

    term.onData((data) => {
      if (ws.readyState === WebSocket.OPEN) ws.send(data);
    });

    termEl.addEventListener('mouseup', () => window.setTimeout(copySelection, 0));
    termEl.addEventListener('touchend', () => window.setTimeout(copySelection, 0), { passive: true });
    termEl.addEventListener('contextmenu', (event) => {
      if (term.hasSelection && term.hasSelection()) return;
      event.preventDefault();
      pasteFromClipboard();
    });

    const onResize = () => {
      fit.fit();
      sendResize();
    };
    window.addEventListener('resize', onResize);

    return {
      close: () => {
        try { ws.close(); } catch (err) {}
        window.removeEventListener('resize', onResize);
        try { term.dispose(); } catch (err) {}
      },
    };
  };

  window.openDevtoolsTerminalPopup = async (resourceId, options = {}) => {
    const popupNamePrefix = String(options.popupNamePrefix || 'terminal_');
    const popupFeatures = String(options.popupFeatures || DEFAULT_FEATURES);
    const wsPath = String(options.wsPath || DEFAULT_WS_PATH);
    const hintText = String(options.hintText || DEFAULT_HINT);
    const title = String(options.title || (resourceId ? (`Resource ${String(resourceId)} terminal`) : 'Terminal'));

    const query = Object.assign({}, options.sessionQuery || {});
    if (resourceId) query.resource_id = String(resourceId);
    if (options.resourceUuid) query.resource_uuid = String(options.resourceUuid);
    const fullWsPath = buildWsPath(wsPath, query);

    const popupName = popupNamePrefix + (resourceId ? String(resourceId) : String(options.popupName || 'session'));
    const popup = window.open('', popupName, popupFeatures);
    if (!popup) return false;

    const html = `<!doctype html><html><head><meta charset="utf-8"><title>${escapeHtml(title)}</title><meta name="viewport" content="width=device-width, initial-scale=1"><style>html,body{height:100%;margin:0;background:#050b12;color:#d7e2f2;font-family:ui-sans-serif,system-ui}.holder{height:100%}</style></head><body><div id="holder" class="holder"></div></body></html>`;
    popup.document.open();
    popup.document.write(html);
    popup.document.close();
    popup.focus();

    const holder = popup.document.getElementById('holder');
    if (!holder) return false;
    const inject = (tag) => popup.document.head.appendChild(tag);

    const css = popup.document.createElement('link');
    css.rel = 'stylesheet';
    css.href = 'https://cdn.jsdelivr.net/npm/xterm/css/xterm.css';
    inject(css);

    const load = (src) => new Promise((resolve, reject) => {
      const s = popup.document.createElement('script');
      s.src = src;
      s.onload = () => resolve();
      s.onerror = () => reject(new Error(`Failed to load ${src}`));
      inject(s);
    });

    try {
      await Promise.all([
        load('https://cdn.jsdelivr.net/npm/xterm/lib/xterm.js'),
        load('https://cdn.jsdelivr.net/npm/xterm-addon-fit/lib/xterm-addon-fit.js'),
        load('https://cdn.jsdelivr.net/npm/xterm-addon-web-links/lib/xterm-addon-web-links.js'),
      ]);

      popup.Terminal = popup.Terminal || popup.window.Terminal;
      popup.FitAddon = popup.FitAddon || popup.window.FitAddon;
      popup.WebLinksAddon = popup.WebLinksAddon || popup.window.WebLinksAddon;

      const client = await (async () => {
        const wrap = popup.document.createElement('div');
        wrap.style.height = '100%';
        holder.appendChild(wrap);

        const term = new popup.Terminal({
          cursorBlink: true,
          scrollback: 50000,
          fontFamily: '"SFMono-Regular","Menlo","Monaco","Consolas","Liberation Mono","Courier New",monospace',
          fontSize: 14,
          theme: { background: '#0a0e12', foreground: '#d7e2f2', cursor: '#6be4a8', selection: 'rgba(107, 228, 168, 0.3)' },
        });
        const fit = new popup.FitAddon.FitAddon();
        term.loadAddon(fit);
        if (popup.WebLinksAddon && popup.WebLinksAddon.WebLinksAddon) {
          term.loadAddon(new popup.WebLinksAddon.WebLinksAddon((event, uri) => {
            if (!shouldActivateTerminalLink(event)) return;
            openTerminalLink(popup, uri);
          }));
        }
        term.open(wrap);
        fit.fit();

        const wsOrigin = window.location.origin.replace(/^http/i, 'ws');
        const ws = new popup.WebSocket(wsOrigin + fullWsPath);
        ws.binaryType = 'arraybuffer';

        const sendResize = () => {
          const dims = fit.proposeDimensions();
          if (!dims || ws.readyState !== popup.WebSocket.OPEN) return;
          ws.send(JSON.stringify({ type: 'resize', cols: dims.cols, rows: dims.rows }));
        };

        ws.onopen = () => sendResize();
        ws.onmessage = (event) => {
          if (event.data instanceof ArrayBuffer) term.write(new Uint8Array(event.data));
          else term.write(event.data);
        };
        ws.onclose = () => term.write('\r\n[terminal session closed]\r\n');

        term.onData((data) => {
          if (ws.readyState === popup.WebSocket.OPEN) ws.send(data);
        });

        const onResize = () => {
          fit.fit();
          sendResize();
        };
        popup.addEventListener('resize', onResize);

        return {
          close: () => {
            try { ws.close(); } catch (err) {}
            popup.removeEventListener('resize', onResize);
            try { term.dispose(); } catch (err) {}
          },
        };
      })();

      popup.addEventListener('beforeunload', () => client.close());
      return true;
    } catch (error) {
      try {
        popup.document.body.innerHTML = '<div style="padding:16px;font:14px/1.4 ui-sans-serif,system-ui;color:#d7e2f2;background:#050b12">Failed to start terminal.</div>';
      } catch (err) {}
      return false;
    }
  };

  const removeAskWidget = () => {
    if (askWidgetDragCleanup) {
      askWidgetDragCleanup();
      askWidgetDragCleanup = null;
    }
    if (askClient) {
      askClient.close();
      askClient = null;
    }
    if (askWidget && askWidget.parentNode) {
      askWidget.parentNode.removeChild(askWidget);
    }
    askWidget = null;
    document.body.classList.remove('ask-widget-open');
  };

  const clearEmbeddedAskWidget = (container) => {
    if (!container || container.nodeType !== 1) return;
    const cleanup = embeddedAskMounts.get(container);
    if (typeof cleanup === 'function') {
      try {
        cleanup();
      } catch (err) {}
    }
    embeddedAskMounts.delete(container);
    container.innerHTML = '';
  };

  const setupDraggableAskWidget = (widget) => {
    const mobileQuery = window.matchMedia('(max-width: 767px)');
    if (mobileQuery.matches) return () => {};
    const header = widget.querySelector('.ask-terminal-widget__head');
    if (!header) return () => {};

    let dragging = false;
    let startX = 0;
    let startY = 0;
    let startLeft = 0;
    let startTop = 0;
    let wasMobileViewport = mobileQuery.matches;

    const clearInlinePosition = () => {
      widget.style.left = '';
      widget.style.top = '';
      widget.style.right = '';
      widget.style.bottom = '';
    };

    const clampPosition = (left, top) => {
      const rect = widget.getBoundingClientRect();
      const maxLeft = Math.max(8, window.innerWidth - rect.width - 8);
      const maxTop = Math.max(8, window.innerHeight - rect.height - 8);
      return {
        left: Math.min(Math.max(8, left), maxLeft),
        top: Math.min(Math.max(8, top), maxTop),
      };
    };

    const ensureAbsolutePosition = () => {
      const rect = widget.getBoundingClientRect();
      widget.style.left = `${Math.max(8, rect.left)}px`;
      widget.style.top = `${Math.max(8, rect.top)}px`;
      widget.style.right = 'auto';
      widget.style.bottom = 'auto';
    };

    const onPointerMove = (event) => {
      if (!dragging) return;
      const nextLeft = startLeft + (event.clientX - startX);
      const nextTop = startTop + (event.clientY - startY);
      const clamped = clampPosition(nextLeft, nextTop);
      widget.style.left = `${clamped.left}px`;
      widget.style.top = `${clamped.top}px`;
      widget.style.right = 'auto';
      widget.style.bottom = 'auto';
    };

    const stopDragging = () => {
      if (!dragging) return;
      dragging = false;
      header.classList.remove('is-dragging');
      document.body.classList.remove('ask-terminal-dragging');
      window.removeEventListener('pointermove', onPointerMove);
      window.removeEventListener('pointerup', stopDragging);
      window.removeEventListener('pointercancel', stopDragging);
    };

    const onPointerDown = (event) => {
      if (mobileQuery.matches) return;
      if (event.button !== 0) return;
      if (event.target && event.target.closest('.ask-terminal-widget__close')) return;
      if (event.target && event.target.closest('.ask-terminal-widget__sudo')) return;
      event.preventDefault();
      ensureAbsolutePosition();
      const rect = widget.getBoundingClientRect();
      dragging = true;
      startX = event.clientX;
      startY = event.clientY;
      startLeft = rect.left;
      startTop = rect.top;
      header.classList.add('is-dragging');
      document.body.classList.add('ask-terminal-dragging');
      window.addEventListener('pointermove', onPointerMove);
      window.addEventListener('pointerup', stopDragging);
      window.addEventListener('pointercancel', stopDragging);
    };

    const keepInBounds = () => {
      const isMobileViewport = mobileQuery.matches;
      if (isMobileViewport) {
        clearInlinePosition();
        wasMobileViewport = true;
        return;
      }
      if (wasMobileViewport !== isMobileViewport) {
        clearInlinePosition();
        wasMobileViewport = isMobileViewport;
        return;
      }
      if (!widget.style.left && !widget.style.top) return;
      const rect = widget.getBoundingClientRect();
      const clamped = clampPosition(rect.left, rect.top);
      const moved = Math.abs(clamped.left - rect.left) > 0.5 || Math.abs(clamped.top - rect.top) > 0.5;
      if (!moved) return;
      widget.style.left = `${clamped.left}px`;
      widget.style.top = `${clamped.top}px`;
      widget.style.right = 'auto';
      widget.style.bottom = 'auto';
    };

    header.addEventListener('pointerdown', onPointerDown);
    window.addEventListener('resize', keepInBounds);

    return () => {
      stopDragging();
      header.removeEventListener('pointerdown', onPointerDown);
      window.removeEventListener('resize', keepInBounds);
    };
  };

  const openAskPopoutWindow = () => {
    const popupUrl = new URL(ASK_POPOUT_PATH, window.location.origin).toString();
    const popout = window.open(popupUrl, 'alshival-ask-widget', ASK_POPOUT_FEATURES);
    if (!popout) return false;
    try {
      popout.focus();
    } catch (err) {}
    return true;
  };

  const buildAskChatWidgetMarkup = ({ title, includeClose, includePopout }) => `
      <div class="ask-terminal-widget__head">
        <span class="ask-terminal-widget__spacer" aria-hidden="true"></span>
        ${isStaff ? '<a href="#" class="ask-terminal-widget__sudo" aria-label="Open terminal hacker mode">Hacker Mode</a>' : ''}
        <button type="button" class="ask-terminal-widget__clear" aria-label="Clear chat history" title="Clear chat history" data-ask-clear>
          <svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" viewBox="0 0 24 24" aria-hidden="true">
            <path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7h16m-10 0V5a1 1 0 0 1 1-1h2a1 1 0 0 1 1 1v2m-7 0l1 12a1 1 0 0 0 1 .9h6a1 1 0 0 0 1-.9l1-12M10 11v6m4-6v6"></path>
          </svg>
        </button>
        ${includePopout ? '<button type="button" class="ask-terminal-widget__popout" aria-label="Open chat in pop-out window" title="Open in pop-out window">Pop out</button>' : ''}
        ${includeClose ? '<button type="button" class="ask-terminal-widget__close" aria-label="Close chat">×</button>' : ''}
      </div>
      <div class="ask-terminal-widget__body ask-chat-widget">
        <div class="ask-chat-widget__messages" aria-live="polite"></div>
        <form class="ask-chat-widget__composer">
          <div class="resource-note-chat">
            <textarea
              rows="1"
              maxlength="8000"
              class="resource-note-input-text"
              data-ask-input
              placeholder="Ask Alshival..."></textarea>
            <div class="resource-note-upload-hints" aria-hidden="true">
              <svg
                class="resource-note-upload-icon"
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                viewBox="0 0 24 24">
                <g fill="none" stroke="currentColor" stroke-width="2">
                  <circle cx="12" cy="13" r="3"></circle>
                  <path d="M9.778 21h4.444c3.121 0 4.682 0 5.803-.735a4.4 4.4 0 0 0 1.226-1.204c.749-1.1.749-2.633.749-5.697s0-4.597-.749-5.697a4.4 4.4 0 0 0-1.226-1.204c-.72-.473-1.622-.642-3.003-.702c-.659 0-1.226-.49-1.355-1.125A2.064 2.064 0 0 0 13.634 3h-3.268c-.988 0-1.839.685-2.033 1.636c-.129.635-.696 1.125-1.355 1.125c-1.38.06-2.282.23-3.003.702A4.4 4.4 0 0 0 2.75 7.667C2 8.767 2 10.299 2 13.364s0 4.596.749 5.697c.324.476.74.885 1.226 1.204C5.096 21 6.657 21 9.778 21Z"></path>
                </g>
              </svg>
              <svg
                class="resource-note-upload-icon"
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                viewBox="0 0 24 24">
                <g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2">
                  <rect width="18" height="18" x="3" y="3" rx="2" ry="2"></rect>
                  <circle cx="9" cy="9" r="2"></circle>
                  <path d="m21 15l-3.086-3.086a2 2 0 0 0-2.828 0L6 21"></path>
                </g>
              </svg>
              <svg
                class="resource-note-upload-icon"
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                viewBox="0 0 24 24">
                <path
                  fill="none"
                  stroke="currentColor"
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="m6 14l1.5-2.9A2 2 0 0 1 9.24 10H20a2 2 0 0 1 1.94 2.5l-1.54 6a2 2 0 0 1-1.95 1.5H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h3.9a2 2 0 0 1 1.69.9l.81 1.2a2 2 0 0 0 1.67.9H18a2 2 0 0 1 2 2v2"></path>
              </svg>
            </div>
            <button class="resource-note-label-send" type="submit" aria-label="Start voice call" data-ask-send data-ask-action="call">
              <svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24">
                <path fill="none" stroke="currentColor" stroke-linecap="round" stroke-width="2" d="M12 4v16m4-13v10M8 7v10m12-6v2M4 11v2"></path>
              </svg>
            </button>
          </div>
        </form>
      </div>
    `;

  const initAskChatWidget = ({ widget, autoFocus, onClose, onSudo, onPopout }) => {
    const closeButton = widget.querySelector('.ask-terminal-widget__close');
    if (closeButton && typeof onClose === 'function') {
      closeButton.addEventListener('click', onClose);
    }
    const sudoButton = widget.querySelector('.ask-terminal-widget__sudo');
    if (sudoButton && typeof onSudo === 'function') {
      sudoButton.addEventListener('click', async (event) => {
        event.preventDefault();
        await onSudo();
      });
    }
    const popoutButton = widget.querySelector('.ask-terminal-widget__popout');
    if (popoutButton && typeof onPopout === 'function') {
      popoutButton.addEventListener('click', (event) => {
        event.preventDefault();
        onPopout();
      });
    }
    const clearButton = widget.querySelector('[data-ask-clear]');

    const messagesEl = widget.querySelector('.ask-chat-widget__messages');
    const formEl = widget.querySelector('.ask-chat-widget__composer');
    const inputEl = widget.querySelector('[data-ask-input]');
    const sendEl = widget.querySelector('[data-ask-send]');
    if (!messagesEl || !formEl || !inputEl || !sendEl) {
      return { close: () => {} };
    }

    let pending = false;
    let chatInitialized = false;
    let historyLoaded = false;
    let voiceState = 'idle';
    let activeCall = null;
    let streamingBubble = null;
    let streamingBubbleBody = null;
    let streamingText = '';
    let assistantLogged = false;
    const audioSink = document.createElement('audio');
    audioSink.autoplay = true;
    audioSink.playsInline = true;
    audioSink.style.display = 'none';
    widget.appendChild(audioSink);

    const ACTION_ICONS = {
      voice: `
        <svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" aria-hidden="true">
          <path fill="none" stroke="currentColor" stroke-linecap="round" stroke-width="2" d="M12 4v16m4-13v10M8 7v10m12-6v2M4 11v2"></path>
        </svg>
      `,
      send: `
        <svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" aria-hidden="true">
          <path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 12l7-7l7 7m-7 7V5"></path>
        </svg>
      `,
      stop: `
        <svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" aria-hidden="true">
          <path fill="none" stroke="currentColor" stroke-linecap="round" stroke-width="2" d="M6 6l12 12M18 6l-12 12"></path>
        </svg>
      `,
      loading: `
        <svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" aria-hidden="true">
          <circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-dasharray="56.5" stroke-dashoffset="42"></circle>
        </svg>
      `,
    };

    const addMessage = (role, text) => {
      const content = String(text || '').trim();
      if (!content) return Promise.resolve(null);
      const node = document.createElement('div');
      node.className = `ask-chat-msg ${role === 'user' ? 'ask-chat-msg--user' : 'ask-chat-msg--assistant'}`;
      const body = document.createElement('div');
      body.className = 'ask-chat-msg__body alshival-markdown alshival-markdown--chat';
      node.appendChild(body);
      messagesEl.appendChild(node);
      if (!chatInitialized) {
        messagesEl.scrollTop = 0;
        chatInitialized = true;
      }
      const rendered = renderMarkdownInto(body, content, { breaks: true }).catch(() => {
        body.textContent = content;
      });
      rendered.finally(() => {
        messagesEl.scrollTop = messagesEl.scrollHeight;
      });
      return rendered;
    };

    const loadChatHistory = async () => {
      if (historyLoaded) return;
      historyLoaded = true;
      const fetchHistory = async () => {
        const params = new URLSearchParams({ conversation_id: 'default', limit: '60' });
        const response = await fetch(`${ASK_CHAT_HISTORY_ENDPOINT}?${params.toString()}`, {
          method: 'GET',
          credentials: 'same-origin',
          headers: { 'X-Requested-With': 'XMLHttpRequest' },
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || !payload || payload.ok !== true || !Array.isArray(payload.messages)) {
          return [];
        }
        return payload.messages
          .filter((item) => item && typeof item === 'object')
          .map((item) => ({
            role: String(item.role || '').trim().toLowerCase(),
            content: String(item.content || '').trim(),
          }))
          .filter((item) => (item.role === 'user' || item.role === 'assistant') && item.content);
      };
      try {
        let history = await fetchHistory();
        if (!history.length) {
          await fetch(ASK_SESSION_GREETING_ENDPOINT, {
            method: 'GET',
            credentials: 'same-origin',
            headers: { 'X-Requested-With': 'XMLHttpRequest' },
          }).catch(() => {});
          history = await fetchHistory();
        }
        if (!history.length) return;
        messagesEl.innerHTML = '';
        chatInitialized = false;
        history.forEach((item) => addMessage(item.role, item.content));
        messagesEl.scrollTop = messagesEl.scrollHeight;
      } catch (error) {}
    };

    const getPageContextText = () => {
      const selected = String((window.getSelection && window.getSelection().toString()) || '').trim();
      const title = String(document.title || '').trim();
      const bodyText = String((document.body && (document.body.innerText || document.body.textContent)) || '')
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, 3000);
      return [selected ? `Selected text: ${selected}` : '', title ? `Page title: ${title}` : '', bodyText ? `Page text: ${bodyText}` : '']
        .filter(Boolean)
        .join('\n\n')
        .slice(0, 4000);
    };

    const logVoiceMessage = async (role, content) => {
      const payload = {
        role: String(role || '').trim().toLowerCase(),
        content: String(content || '').trim(),
        conversation_id: 'default',
      };
      if (!payload.content) return;
      try {
        await fetch(ASK_VOICE_LOG_ENDPOINT, {
          method: 'POST',
          credentials: 'same-origin',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCookie('csrftoken'),
            'X-Requested-With': 'XMLHttpRequest',
          },
          body: JSON.stringify(payload),
        });
      } catch (error) {}
    };

    const updateActionButton = () => {
      const hasText = String(inputEl.value || '').trim().length > 0;
      sendEl.classList.add('ask-chat-widget__action');
      sendEl.classList.toggle('is-voice-active', voiceState === 'active');
      if (voiceState === 'connecting') {
        sendEl.dataset.askAction = 'call';
        sendEl.setAttribute('aria-label', 'Connecting voice call');
        sendEl.innerHTML = ACTION_ICONS.loading;
        sendEl.disabled = true;
        return;
      }
      if (voiceState === 'active') {
        sendEl.dataset.askAction = 'call';
        sendEl.setAttribute('aria-label', 'End voice call');
        sendEl.innerHTML = ACTION_ICONS.stop;
        sendEl.disabled = false;
        return;
      }
      if (hasText) {
        sendEl.dataset.askAction = 'send';
        sendEl.setAttribute('aria-label', 'Send message');
        sendEl.innerHTML = ACTION_ICONS.send;
        sendEl.disabled = pending;
        return;
      }
      sendEl.dataset.askAction = 'call';
      sendEl.setAttribute('aria-label', 'Start voice call');
      sendEl.innerHTML = ACTION_ICONS.voice;
      sendEl.disabled = pending;
    };

    const setVoiceState = (nextState) => {
      voiceState = String(nextState || 'idle');
      inputEl.disabled = pending || voiceState !== 'idle';
      updateActionButton();
    };

    const setPending = (next) => {
      pending = Boolean(next);
      inputEl.disabled = pending || voiceState !== 'idle';
      if (clearButton) clearButton.disabled = pending;
      updateActionButton();
    };

    if (clearButton) {
      clearButton.addEventListener('click', async (event) => {
        event.preventDefault();
        if (pending) return;
        const confirmed = window.confirm('Clear this chat history? This cannot be undone.');
        if (!confirmed) return;
        setPending(true);
        try {
          const response = await fetch(ASK_CHAT_HISTORY_CLEAR_ENDPOINT, {
            method: 'POST',
            credentials: 'same-origin',
            headers: {
              'Content-Type': 'application/json',
              'X-CSRFToken': getCookie('csrftoken'),
              'X-Requested-With': 'XMLHttpRequest',
            },
            body: JSON.stringify({ conversation_id: 'default' }),
          });
          const payload = await response.json().catch(() => ({}));
          if (!response.ok || !payload || payload.ok !== true) {
            throw new Error(String(payload.error || 'request_failed'));
          }
          messagesEl.innerHTML = '';
          chatInitialized = false;
          historyLoaded = false;
          await loadChatHistory();
        } catch (error) {
          addMessage('assistant', 'Unable to clear chat history right now.');
        } finally {
          setPending(false);
          inputEl.focus();
        }
      });
    }

    inputEl.addEventListener('keydown', (event) => {
      if (event.key !== 'Enter') return;
      if (event.shiftKey) return;
      if (event.isComposing) return;
      if (!String(inputEl.value || '').trim()) return;
      event.preventDefault();
      if (typeof formEl.requestSubmit === 'function') {
        formEl.requestSubmit();
        return;
      }
      formEl.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
    });

    const connectVoice = async () => {
      if (activeCall && typeof activeCall.cleanup === 'function') {
        activeCall.cleanup();
        addMessage('assistant', 'Voice call ended.');
        return;
      }
      if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia || typeof window.RTCPeerConnection !== 'function') {
        addMessage('assistant', 'Voice calls are unavailable in this browser.');
        return;
      }
      setVoiceState('connecting');
      addMessage('assistant', 'Starting a voice call...');
      try {
        const tokenResponse = await fetch(ASK_VOICE_TOKEN_ENDPOINT, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCookie('csrftoken'),
            'X-Requested-With': 'XMLHttpRequest',
          },
          body: JSON.stringify({
            page_url: window.location && window.location.href ? window.location.href : '',
            page_text: getPageContextText(),
          }),
        });
        const tokenPayload = await tokenResponse.json().catch(() => ({}));
        if (!tokenResponse.ok) {
          throw new Error(String(tokenPayload.error || 'voice_token_failed'));
        }
        const clientSecret = String(tokenPayload.client_secret || '').trim();
        const model = String(tokenPayload.model || '').trim() || 'gpt-4o-realtime-preview';
        const voice = String(tokenPayload.voice || '').trim() || 'alloy';
        const instructions = String(tokenPayload.instructions || '').trim();
        if (!clientSecret) throw new Error('voice_credentials_missing');

        const pc = new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] });
        pc.addTransceiver('audio', { direction: 'sendrecv' });
        pc.addEventListener('track', (event) => {
          if (event.track.kind !== 'audio') return;
          const inbound = new MediaStream([event.track]);
          audioSink.srcObject = inbound;
          audioSink.play().catch(() => {});
        });

        const micStream = await navigator.mediaDevices.getUserMedia({ audio: true });
        micStream.getTracks().forEach((track) => pc.addTrack(track, micStream));
        let dc = pc.createDataChannel('oai-events');
        dc.binaryType = 'arraybuffer';

        const cleanup = () => {
          try { dc && dc.close(); } catch (err) {}
          try { pc.close(); } catch (err) {}
          try { micStream.getTracks().forEach((track) => track.stop()); } catch (err) {}
          try { audioSink.srcObject = null; } catch (err) {}
          activeCall = null;
          streamingBubble = null;
          streamingBubbleBody = null;
          streamingText = '';
          assistantLogged = false;
          setVoiceState('idle');
        };

        const setupDataChannelHandlers = () => {
          if (!dc) return;
          dc.addEventListener('open', () => {
            setVoiceState('active');
            dc.send(JSON.stringify({
              type: 'session.update',
              session: {
                input_audio_transcription: { model: 'gpt-4o-mini-transcribe' },
                voice,
                model,
                instructions,
              },
            }));
            dc.send(JSON.stringify({
              type: 'response.create',
              response: {
                modalities: ['text', 'audio'],
                instructions: 'Use voice to assist the user.',
                voice,
              },
            }));
          }, { once: true });

          dc.addEventListener('message', (ev) => {
            let payload = null;
            try { payload = JSON.parse(ev.data); } catch (err) {}
            if (!payload || typeof payload !== 'object') return;
            if (payload.type === 'response.created') {
              streamingBubble = null;
              streamingBubbleBody = null;
              streamingText = '';
              assistantLogged = false;
              return;
            }
            if (payload.type === 'response.audio_transcript.delta' && payload.delta) {
              if (!streamingBubble) {
                streamingBubble = document.createElement('div');
                streamingBubble.className = 'ask-chat-msg ask-chat-msg--assistant';
                streamingBubbleBody = document.createElement('div');
                streamingBubbleBody.className = 'ask-chat-msg__body alshival-markdown alshival-markdown--chat';
                streamingBubble.appendChild(streamingBubbleBody);
                messagesEl.appendChild(streamingBubble);
              }
              streamingText += String(payload.delta || '');
              if (streamingBubbleBody) {
                streamingBubbleBody.textContent = streamingText;
              }
              messagesEl.scrollTop = messagesEl.scrollHeight;
              return;
            }
            if (payload.type === 'response.audio_transcript.done') {
              if (streamingBubbleBody && streamingText) {
                renderMarkdownInto(streamingBubbleBody, streamingText, { breaks: true }).catch(() => {});
              }
              if (!assistantLogged && streamingText) {
                logVoiceMessage('assistant', streamingText);
                assistantLogged = true;
              }
              streamingBubble = null;
              streamingBubbleBody = null;
              streamingText = '';
              return;
            }
            if (payload.type === 'response.message' && Array.isArray(payload.content)) {
              const chunks = payload.content
                .map((part) => String((part && (part.text || part.transcript || '')) || '').trim())
                .filter(Boolean);
              if (!chunks.length) return;
              const combined = chunks.join('\n');
              addMessage('assistant', combined);
              logVoiceMessage('assistant', combined);
              assistantLogged = true;
              streamingBubble = null;
              streamingBubbleBody = null;
              streamingText = '';
              return;
            }
            if (
              payload.type === 'input_audio_transcription.done'
              || payload.type === 'input_audio_transcription.completed'
              || payload.type === 'input_audio_transcript.done'
            ) {
              const transcript = String(payload.transcript || payload.text || '').trim();
              if (!transcript) return;
              addMessage('user', transcript);
              logVoiceMessage('user', transcript);
            }
          });
        };

        pc.addEventListener('datachannel', (ev) => {
          dc = ev.channel;
          dc.binaryType = 'arraybuffer';
          setupDataChannelHandlers();
        });
        setupDataChannelHandlers();

        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        const sdpResponse = await fetch('https://api.openai.com/v1/realtime/calls', {
          method: 'POST',
          body: offer.sdp,
          headers: {
            Authorization: `Bearer ${clientSecret}`,
            'Content-Type': 'application/sdp',
            'OpenAI-Beta': 'realtime=v1',
          },
        });
        if (!sdpResponse.ok) {
          throw new Error(`sdp_error_${sdpResponse.status}`);
        }
        const answerSdp = await sdpResponse.text();
        await pc.setRemoteDescription({ type: 'answer', sdp: answerSdp });
        dc.addEventListener('close', cleanup);
        dc.addEventListener('error', cleanup);
        activeCall = { pc, dc, cleanup };
      } catch (error) {
        setVoiceState('idle');
        addMessage('assistant', 'Unable to start a voice call right now.');
      }
    };

    formEl.addEventListener('submit', async (event) => {
      event.preventDefault();
      if (pending) return;
      const message = String(inputEl.value || '').trim();
      const action = String(sendEl.dataset.askAction || '').trim().toLowerCase();
      const isCallSubmit = action === 'call' && (!message) && (event.submitter === sendEl);
      if (isCallSubmit) {
        await connectVoice();
        return;
      }
      if (!message) return;

      addMessage('user', message);
      inputEl.value = '';
      setPending(true);
      try {
        const response = await fetch(ASK_CHAT_ENDPOINT, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCookie('csrftoken'),
            'X-Requested-With': 'XMLHttpRequest',
          },
          body: JSON.stringify({ message }),
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
          addMessage('assistant', `Chat unavailable (${String(payload.error || 'request_failed')}).`);
          return;
        }
        addMessage('assistant', String(payload.reply || '').trim() || 'No response.');
      } catch (error) {
        addMessage('assistant', 'Chat unavailable right now.');
      } finally {
        setPending(false);
        inputEl.focus();
      }
    });

    inputEl.addEventListener('input', () => {
      updateActionButton();
    });
    sendEl.addEventListener('click', async (event) => {
      const action = String(sendEl.dataset.askAction || '').trim().toLowerCase();
      const message = String(inputEl.value || '').trim();
      if (action !== 'call' || message) return;
      event.preventDefault();
      if (pending) return;
      await connectVoice();
    });

    updateActionButton();
    loadChatHistory().catch(() => {});
    if (autoFocus) {
      inputEl.focus();
    }
    return {
      close: () => {
        if (activeCall && typeof activeCall.cleanup === 'function') {
          activeCall.cleanup();
        }
      },
    };
  };

  const openAskChatWidget = async ({ title }) => {
    removeAskWidget();

    askWidget = document.createElement('section');
    askWidget.className = 'ask-terminal-widget';
    askWidget.innerHTML = buildAskChatWidgetMarkup({ title, includeClose: true, includePopout: true });
    document.body.appendChild(askWidget);
    document.body.classList.add('ask-widget-open');
    askWidgetDragCleanup = setupDraggableAskWidget(askWidget);

    askClient = initAskChatWidget({
      widget: askWidget,
      autoFocus: true,
      onClose: () => removeAskWidget(),
      onSudo: async () => {
        await openAskWidget({
          mode: 'shell',
          title: 'System Terminal',
          hintText: 'Staff local login shell',
        });
      },
      onPopout: () => {
        const opened = openAskPopoutWindow();
        if (opened) {
          removeAskWidget();
        }
      },
    });
  };

  const openAskWidget = async ({ mode, title, hintText }) => {
    removeAskWidget();

    askWidget = document.createElement('section');
    askWidget.className = 'ask-terminal-widget';
    askWidget.innerHTML = `
      <div class="ask-terminal-widget__head">
        <strong>${escapeHtml(title)}</strong>
        <button type="button" class="ask-terminal-widget__close" aria-label="Close terminal">×</button>
      </div>
      <div class="ask-terminal-widget__body"></div>
    `;
    document.body.appendChild(askWidget);
    document.body.classList.add('ask-widget-open');
    askWidgetDragCleanup = setupDraggableAskWidget(askWidget);

    const closeButton = askWidget.querySelector('.ask-terminal-widget__close');
    closeButton.addEventListener('click', () => removeAskWidget());

    const body = askWidget.querySelector('.ask-terminal-widget__body');
    const wsPath = buildWsPath(DEFAULT_WS_PATH, { mode });

    try {
      askClient = await makeTerminalClient({
        container: body,
        title,
        hintText,
        wsPath,
        onSocketClose: ({ opened, durationMs }) => {
          const shouldFallback = mode === 'shell' && (!opened || durationMs < 2000);
          if (!shouldFallback || !askWidget || !askWidget.isConnected) return;
          window.setTimeout(() => {
            if (!askWidget || !askWidget.isConnected) return;
            openAskChatWidget({ title: 'Ask Alshival' });
          }, 200);
        },
      });
    } catch (error) {
      if (mode === 'shell') {
        await openAskChatWidget({ title: 'Ask Alshival' });
        return;
      }
      body.innerHTML = '<div class="ask-terminal-widget__error">Failed to start terminal.</div>';
    }
  };

  const mountEmbeddedShellWidget = async ({ container, showPopout, showClose }) => {
    if (!container || container.nodeType !== 1) return false;
    clearEmbeddedAskWidget(container);

    const restoreChat = () => {
      window.mountAskAlshivalWidget({
        container,
        title: 'Ask Alshival',
        autoFocus: false,
        showPopout,
        showClose,
        inlineShell: true,
      }).catch(() => {});
    };

    container.innerHTML = '';
    const shellWidget = document.createElement('section');
    shellWidget.className = 'ask-terminal-widget ask-terminal-widget--embedded';
    shellWidget.innerHTML = `
      <div class="ask-terminal-widget__head">
        <strong>System Terminal</strong>
        <button type="button" class="ask-terminal-widget__chatback" aria-label="Return to Ask Alshival">Ask Alshival</button>
        ${showPopout ? '<button type="button" class="ask-terminal-widget__popout" aria-label="Open chat in pop-out window" title="Open in pop-out window">Pop out</button>' : ''}
        <button type="button" class="ask-terminal-widget__close" aria-label="Close terminal">×</button>
      </div>
      <div class="ask-terminal-widget__body"></div>
    `;
    container.appendChild(shellWidget);

    const closeButton = shellWidget.querySelector('.ask-terminal-widget__close');
    if (closeButton) closeButton.addEventListener('click', restoreChat);
    const backButton = shellWidget.querySelector('.ask-terminal-widget__chatback');
    if (backButton) backButton.addEventListener('click', restoreChat);
    const popoutButton = shellWidget.querySelector('.ask-terminal-widget__popout');
    if (popoutButton) {
      popoutButton.addEventListener('click', (event) => {
        event.preventDefault();
        const opened = openAskPopoutWindow();
        if (opened) {
          clearEmbeddedAskWidget(container);
        }
      });
    }

    const body = shellWidget.querySelector('.ask-terminal-widget__body');
    const wsPath = buildWsPath(DEFAULT_WS_PATH, { mode: 'shell' });
    let shellClient = null;
    try {
      shellClient = await makeTerminalClient({
        container: body,
        title: 'System Terminal',
        hintText: 'Staff local login shell',
        wsPath,
      });
    } catch (error) {
      body.innerHTML = '<div class="ask-terminal-widget__error">Failed to start terminal.</div>';
    }

    embeddedAskMounts.set(container, () => {
      if (shellClient && typeof shellClient.close === 'function') {
        shellClient.close();
      }
    });
    return true;
  };

  window.openAskAlshivalWidget = async (options = {}) => {
    const mode = String((options && options.mode) || 'chat').trim().toLowerCase();
    const title = String((options && options.title) || 'Ask Alshival');
    const hintText = String((options && options.hintText) || DEFAULT_HINT);
    if (mode === 'shell') {
      if (!isStaff) return false;
      await openAskWidget({
        mode: 'shell',
        title,
        hintText,
      });
      return true;
    }
    await openAskChatWidget({ title });
    return true;
  };
  window.openAskAlshivalWidgetPopout = () => {
    const opened = openAskPopoutWindow();
    if (opened) {
      removeAskWidget();
    }
    return opened;
  };

  window.mountAskAlshivalWidget = async (options = {}) => {
    const target = options && options.container;
    const container = typeof target === 'string'
      ? document.querySelector(target)
      : (target && target.nodeType === 1 ? target : null);
    if (!container) return false;

    clearEmbeddedAskWidget(container);

    const title = String((options && options.title) || 'Ask Alshival');
    const showPopout = !(options && options.showPopout === false);
    const showClose = !(options && options.showClose === false);
    const inlineShell = Boolean(options && options.inlineShell);
    container.innerHTML = '';
    const embeddedWidget = document.createElement('section');
    embeddedWidget.className = 'ask-terminal-widget ask-terminal-widget--embedded';
    embeddedWidget.innerHTML = buildAskChatWidgetMarkup({ title, includeClose: showClose, includePopout: showPopout });
    container.appendChild(embeddedWidget);

    const client = initAskChatWidget({
      widget: embeddedWidget,
      autoFocus: Boolean(options && options.autoFocus),
      onClose: () => {
        window.mountAskAlshivalWidget({
          container,
          title,
          autoFocus: false,
          showPopout,
          showClose,
        }).catch(() => {});
      },
      onSudo: async () => {
        if (!isStaff) return;
        if (inlineShell) {
          await mountEmbeddedShellWidget({
            container,
            showPopout,
            showClose,
          });
          return;
        }
        await openAskWidget({
          mode: 'shell',
          title: 'System Terminal',
          hintText: 'Staff local login shell',
        });
      },
      onPopout: () => {
        const opened = openAskPopoutWindow();
        if (opened) {
          clearEmbeddedAskWidget(container);
        }
      },
    });
    embeddedAskMounts.set(container, () => {
      if (client && typeof client.close === 'function') {
        client.close();
      }
    });
    return true;
  };

  const askButton = document.querySelector('.floating-ask-alshival');
  if (askButton) {
    window.showAlshivalAgentBubble = (markdownText, options = {}) => showAgentBubble(askButton, markdownText, options);
    window.hideAlshivalAgentBubble = () => {
      hideAgentBubble();
      return true;
    };
    window.AlshivalAgentBubble = {
      show: window.showAlshivalAgentBubble,
      hide: window.hideAlshivalAgentBubble,
    };
    window.addEventListener('alshival:agent-bubble', (event) => {
      const detail = event && event.detail && typeof event.detail === 'object' ? event.detail : {};
      const markdown = String((detail && (detail.markdown || detail.message || '')) || '').trim();
      if (!markdown) return;
      window.showAlshivalAgentBubble(markdown, detail);
    });

    window.setTimeout(() => {
      maybeShowSessionGreeting(askButton).catch(() => {});
    }, 220);

    askButton.addEventListener('click', async (event) => {
      event.preventDefault();
      hideAgentBubble();
      await window.openAskAlshivalWidget({ mode: 'chat', title: 'Ask Alshival' });
    });
  }
})();
