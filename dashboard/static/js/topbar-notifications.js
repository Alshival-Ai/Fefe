(() => {
  const menu = document.querySelector('[data-notification-menu]');
  if (!menu) {
    return;
  }

  const toggleBtn = menu.querySelector('[data-notification-toggle]');
  const dropdown = menu.querySelector('[data-notification-dropdown]');
  const listContainer = menu.querySelector('[data-notification-list]');
  const badge = menu.querySelector('[data-notification-badge]');
  const markReadBtn = menu.querySelector('[data-notification-mark-read]');
  const clearAllBtn = menu.querySelector('[data-notification-clear-all]');
  const listUrl = menu.getAttribute('data-list-url') || '';
  const markReadUrl = menu.getAttribute('data-mark-read-url') || '';
  const clearUrl = menu.getAttribute('data-clear-url') || '';

  if (!toggleBtn || !dropdown || !listContainer || !badge || !listUrl) {
    return;
  }

  let isOpen = false;

  function getCookie(name) {
    const cookieString = document.cookie || '';
    if (!cookieString) {
      return '';
    }
    const parts = cookieString.split(';');
    for (let i = 0; i < parts.length; i += 1) {
      const part = parts[i].trim();
      if (part.startsWith(`${name}=`)) {
        return decodeURIComponent(part.slice(name.length + 1));
      }
    }
    return '';
  }

  function formatWhen(value) {
    if (!value) {
      return '';
    }
    const normalized = value.includes('T') ? value : value.replace(' ', 'T');
    const parsed = new Date(normalized.endsWith('Z') ? normalized : `${normalized}Z`);
    if (Number.isNaN(parsed.getTime())) {
      return value;
    }
    try {
      return new Intl.DateTimeFormat([], {
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
      }).format(parsed);
    } catch (error) {
      return parsed.toLocaleString();
    }
  }

  function setBadge(unreadCount) {
    const value = Number.isFinite(unreadCount) ? Math.max(0, unreadCount) : 0;
    if (value > 0) {
      badge.textContent = value > 99 ? '99+' : String(value);
      badge.classList.remove('d-none');
      return;
    }
    badge.textContent = '0';
    badge.classList.add('d-none');
  }

  function clearList() {
    while (listContainer.firstChild) {
      listContainer.removeChild(listContainer.firstChild);
    }
  }

  function renderEmpty(message) {
    clearList();
    const empty = document.createElement('p');
    empty.className = 'notification-empty';
    empty.textContent = message;
    listContainer.appendChild(empty);
  }

  function renderItems(items) {
    clearList();
    if (!Array.isArray(items) || items.length === 0) {
      renderEmpty('No alerts yet.');
      return;
    }

    items.forEach((item) => {
      const level = String(item && item.level ? item.level : 'info').toLowerCase();
      const title = String(item && item.title ? item.title : 'Notification');
      const body = String(item && item.body ? item.body : '');
      const createdAt = String(item && item.created_at ? item.created_at : '');

      const card = document.createElement('article');
      card.className = `notification-item notification-item--${level}`;

      const head = document.createElement('header');
      head.className = 'notification-item__head';

      const titleNode = document.createElement('h4');
      titleNode.className = 'notification-item__title';
      titleNode.textContent = title;

      const timeNode = document.createElement('time');
      timeNode.className = 'notification-item__time';
      timeNode.textContent = formatWhen(createdAt);

      head.appendChild(titleNode);
      head.appendChild(timeNode);

      const bodyNode = document.createElement('p');
      bodyNode.className = 'notification-item__body';
      bodyNode.textContent = body;

      card.appendChild(head);
      card.appendChild(bodyNode);
      listContainer.appendChild(card);
    });
  }

  function setOpen(nextOpen) {
    isOpen = !!nextOpen;
    dropdown.hidden = !isOpen;
    toggleBtn.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
  }

  async function loadNotifications() {
    try {
      const response = await fetch(`${listUrl}?limit=15`, {
        method: 'GET',
        credentials: 'same-origin',
      });
      if (!response.ok) {
        throw new Error(`notifications_fetch_${response.status}`);
      }
      const payload = await response.json();
      const unread = Number(payload && payload.unread_count ? payload.unread_count : 0);
      const items = payload && payload.items ? payload.items : [];
      setBadge(unread);
      renderItems(items);
    } catch (error) {
      renderEmpty('Unable to load notifications.');
    }
  }

  async function markAllRead() {
    if (!markReadUrl) {
      return;
    }
    try {
      const response = await fetch(markReadUrl, {
        method: 'POST',
        credentials: 'same-origin',
        headers: {
          'X-CSRFToken': getCookie('csrftoken'),
        },
      });
      if (!response.ok) {
        return;
      }
      setBadge(0);
      await loadNotifications();
    } catch (error) {
      // Keep the dropdown usable if mark-read fails.
    }
  }

  async function clearAllNotifications() {
    if (!clearUrl) {
      return;
    }
    try {
      const response = await fetch(clearUrl, {
        method: 'POST',
        credentials: 'same-origin',
        headers: {
          'X-CSRFToken': getCookie('csrftoken'),
        },
      });
      if (!response.ok) {
        return;
      }
      setBadge(0);
      renderEmpty('No alerts yet.');
    } catch (error) {
      // Keep the dropdown usable if clear fails.
    }
  }

  toggleBtn.addEventListener('click', async () => {
    const nextOpen = !isOpen;
    setOpen(nextOpen);
    if (nextOpen) {
      await loadNotifications();
    }
  });

  if (markReadBtn) {
    markReadBtn.addEventListener('click', async () => {
      await markAllRead();
    });
  }

  if (clearAllBtn) {
    clearAllBtn.addEventListener('click', async () => {
      await clearAllNotifications();
    });
  }

  document.addEventListener('click', (event) => {
    if (!isOpen) {
      return;
    }
    if (!menu.contains(event.target)) {
      setOpen(false);
    }
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && isOpen) {
      setOpen(false);
    }
  });

  loadNotifications();
  window.setInterval(() => {
    if (document.hidden) {
      return;
    }
    loadNotifications();
  }, 30000);
})();
