(() => {
  const WEEKDAY_LABELS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const KIND_ORDER = ['meeting', 'delivery', 'follow-up', 'review'];
  const KIND_LABELS = {
    meeting: 'Meeting',
    delivery: 'Delivery',
    'follow-up': 'Follow-up',
    review: 'Review',
  };

  const toYmd = (input) => {
    const date = input instanceof Date ? input : new Date(input);
    if (Number.isNaN(date.getTime())) return '';
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  };

  const monthFloor = (input) => {
    const date = input instanceof Date ? new Date(input) : new Date(input);
    if (Number.isNaN(date.getTime())) return new Date(new Date().getFullYear(), new Date().getMonth(), 1);
    return new Date(date.getFullYear(), date.getMonth(), 1);
  };

  const parseYmd = (value) => {
    const normalized = String(value || '').trim();
    if (!normalized) return null;
    const parsed = new Date(`${normalized}T00:00:00`);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  };

  const addDays = (value, offset) => {
    const date = value instanceof Date ? new Date(value) : new Date(value);
    if (Number.isNaN(date.getTime())) return null;
    date.setDate(date.getDate() + Number(offset || 0));
    return date;
  };

  const prettyMonth = (value) => {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    try {
      return new Intl.DateTimeFormat([], { month: 'long', year: 'numeric' }).format(date);
    } catch (error) {
      return date.toLocaleDateString();
    }
  };

  const prettyDay = (value) => {
    const parsed = parseYmd(value);
    if (!parsed) return '';
    try {
      return new Intl.DateTimeFormat([], { weekday: 'short', month: 'short', day: 'numeric' }).format(parsed);
    } catch (error) {
      return parsed.toLocaleDateString();
    }
  };

  const prettyTime = (value) => {
    const normalized = String(value || '').trim();
    if (!normalized) return 'All day';
    const parsed = new Date(`1970-01-01T${normalized}`);
    if (Number.isNaN(parsed.getTime())) return normalized;
    try {
      return new Intl.DateTimeFormat([], { hour: 'numeric', minute: '2-digit' }).format(parsed);
    } catch (error) {
      return parsed.toLocaleTimeString();
    }
  };

  const normalizeKind = (value) => {
    const kind = String(value || '').trim().toLowerCase();
    return KIND_ORDER.includes(kind) ? kind : 'follow-up';
  };

  const resolveCanToggle = (item, fallback = true) => {
    if (!item || typeof item !== 'object') return Boolean(fallback);
    const hasCamel = Object.prototype.hasOwnProperty.call(item, 'canToggle');
    const hasSnake = Object.prototype.hasOwnProperty.call(item, 'can_toggle');
    if (!hasCamel && !hasSnake) return Boolean(fallback);
    if (hasCamel) return Boolean(item.canToggle);
    return Boolean(item.can_toggle);
  };

  const normalizeItems = (items, defaults = {}) => {
    if (!Array.isArray(items)) return [];
    return items
      .map((item) => ({
        id: String(item && item.id ? item.id : `item-${Date.now()}-${Math.floor(Math.random() * 10000)}`),
        title: String(item && item.title ? item.title : '').trim(),
        date: String(item && item.date ? item.date : '').trim(),
        time: String(item && item.time ? item.time : '').trim(),
        kind: normalizeKind(item && item.kind ? item.kind : 'follow-up'),
        done: Boolean(item && item.done),
        source: String(item && item.source ? item.source : defaults.source || 'local').trim().toLowerCase() || 'local',
        url: String(item && item.url ? item.url : '').trim(),
        taskGid: String(item && (item.task_gid || item.taskGid) ? (item.task_gid || item.taskGid) : '').trim(),
        isExternal: Boolean(defaults.isExternal || (item && (item.isExternal || item.external))),
        canToggle: resolveCanToggle(item, true),
      }))
      .filter((item) => item.title && parseYmd(item.date))
      .sort((a, b) => `${a.date} ${a.time || '99:99'}`.localeCompare(`${b.date} ${b.time || '99:99'}`));
  };

  const normalizeAgendaSections = (sections) => {
    if (!Array.isArray(sections)) return [];
    return sections
      .map((section, sectionIndex) => {
        const resolvedSectionId = String(
          section && section.id ? section.id : `section-${sectionIndex + 1}`
        ).trim();
        const title = String(section && section.title ? section.title : '').trim();
        if (!resolvedSectionId || !title) return null;
        const description = String(section && section.description ? section.description : '').trim();
        const emptyText = String(
          section && (section.emptyText || section.empty_text)
            ? (section.emptyText || section.empty_text)
            : ''
        ).trim();
        const boardGid = String(section && (section.boardGid || section.board_gid) ? (section.boardGid || section.board_gid) : '').trim();
        const boardName = String(section && (section.boardName || section.board_name) ? (section.boardName || section.board_name) : '').trim();
        const sectionActions = Array.isArray(section && section.actions)
          ? section.actions
            .map((action, actionIndex) => ({
              id: String(action && action.id ? action.id : `section-action-${actionIndex + 1}`).trim(),
              label: String(action && action.label ? action.label : '').trim(),
              title: String(action && action.title ? action.title : '').trim(),
              iconOnly: Boolean(action && action.iconOnly),
            }))
            .filter((action) => action.id && action.label)
          : [];
        const rows = Array.isArray(section && section.items) ? section.items : [];
        const normalizedRows = rows
          .map((row, rowIndex) => {
            const rowId = String(row && row.id ? row.id : `${resolvedSectionId}-row-${rowIndex + 1}`).trim();
            const rowTitle = String(row && row.title ? row.title : '').trim();
            if (!rowId || !rowTitle) return null;
            return {
              id: rowId,
              title: rowTitle,
              meta: String(row && row.meta ? row.meta : '').trim(),
              url: String(row && row.url ? row.url : '').trim(),
              done: Boolean(row && row.done),
              canToggle: Boolean(row && row.canToggle),
              source: String(row && row.source ? row.source : '').trim().toLowerCase(),
              taskGid: String(row && (row.task_gid || row.taskGid) ? (row.task_gid || row.taskGid) : '').trim(),
              isExternal: Boolean(row && (row.isExternal || row.external)),
              inlineComment: Boolean(row && row.inlineComment),
              actions: Array.isArray(row && row.actions)
                ? row.actions
                  .map((action, actionIndex) => ({
                    id: String(action && action.id ? action.id : `action-${actionIndex + 1}`).trim(),
                    label: String(action && action.label ? action.label : '').trim(),
                  }))
                  .filter((action) => action.id && action.label)
                : [],
              badges: Array.isArray(row && row.badges)
                ? row.badges
                  .map((badge, badgeIndex) => ({
                    id: String(badge && badge.id ? badge.id : `badge-${badgeIndex + 1}`).trim(),
                    label: String(badge && badge.label ? badge.label : '').trim(),
                    title: String(badge && badge.title ? badge.title : '').trim(),
                  }))
                  .filter((badge) => badge.id && badge.label)
                : [],
            };
          })
          .filter(Boolean);
        return {
          id: resolvedSectionId,
          title,
          description,
          emptyText,
          boardGid,
          boardName,
          actions: sectionActions,
          items: normalizedRows,
        };
      })
      .filter(Boolean);
  };

  const createId = () => `item-${Date.now()}-${Math.floor(Math.random() * 100000)}`;

  const readStorage = (key) => {
    try {
      return window.localStorage.getItem(key);
    } catch (error) {
      return null;
    }
  };

  const writeStorage = (key, value) => {
    try {
      window.localStorage.setItem(key, value);
      return true;
    } catch (error) {
      return false;
    }
  };

  const resolveStorageKey = (root, options) => {
    if (typeof options.getStorageKey === 'function') {
      return String(options.getStorageKey() || '').trim();
    }
    if (typeof options.storageKey === 'string') {
      return String(options.storageKey || '').trim();
    }
    return String(root.getAttribute('data-planner-storage-key') || 'planner_items').trim();
  };

  const initPlanner = (root, options = {}) => {
    if (!root || root.__plannerController) {
      return root && root.__plannerController ? root.__plannerController : null;
    }

    const grid = root.querySelector('[data-planner-grid]');
    const agenda = root.querySelector('[data-planner-agenda]');
    const monthLabel = root.querySelector('[data-planner-month]');
    const selectedLabel = root.querySelector('[data-planner-selected]');
    const agendaTitle = root.querySelector('[data-planner-agenda-title]');
    const prevButton = root.querySelector('[data-planner-prev]');
    const nextButton = root.querySelector('[data-planner-next]');
    const todayButton = root.querySelector('[data-planner-today]');
    const addForm = root.querySelector('[data-planner-form]');
    const titleInput = root.querySelector('[data-planner-title]');
    const dateInput = root.querySelector('[data-planner-date]');
    const timeInput = root.querySelector('[data-planner-time]');
    const kindInput = root.querySelector('[data-planner-kind]');
    const filterButtons = Array.from(root.querySelectorAll('[data-planner-filter]'));
    const viewButtons = Array.from(root.querySelectorAll('[data-planner-view]'));
    const statNodes = {
      today: root.querySelector('[data-planner-stat="today"]'),
      week: root.querySelector('[data-planner-stat="week"]'),
      done: root.querySelector('[data-planner-stat="done"]'),
    };
    if (!grid || !agenda || !monthLabel || !selectedLabel) return null;

    const seedItems = typeof options.seedItems === 'function' ? options.seedItems : () => [];
    const externalItems = typeof options.externalItems === 'function'
      ? options.externalItems
      : () => (Array.isArray(options.externalItems) ? options.externalItems : []);
    const agendaSections = typeof options.agendaSections === 'function'
      ? options.agendaSections
      : () => (Array.isArray(options.agendaSections) ? options.agendaSections : []);
    const onExternalToggle = typeof options.onExternalToggle === 'function' ? options.onExternalToggle : null;
    const onToggleError = typeof options.onToggleError === 'function' ? options.onToggleError : null;
    const onAgendaSectionAction = typeof options.onAgendaSectionAction === 'function' ? options.onAgendaSectionAction : null;
    const onActionError = typeof options.onActionError === 'function' ? options.onActionError : null;
    const onInlineComment = typeof options.onInlineComment === 'function' ? options.onInlineComment : null;
    const onItemClick = typeof options.onItemClick === 'function' ? options.onItemClick : null;
    const listHistoryDays = Math.max(1, Number.parseInt(String(options.listHistoryDays || '30'), 10) || 30);
    const monthEmptyText = String(options.monthEmptyText || 'No tasks for this day.');
    const listEmptyText = String(options.listEmptyText || 'No tasks in this timeline.');
    const showMonthTimelineItemsOption = options.showMonthTimelineItems;
    const showListTimelineItemsOption = options.showListTimelineItems;
    const readOnly = Boolean(options.readOnly);

    let selectedDate = toYmd(new Date());
    let calendarCursor = monthFloor(new Date());
    let activeFilter = 'all';
    let activeView = 'tasks';
    let localItems = [];
    let remoteItems = [];
    let items = [];
    const togglePendingById = new Set();
    const togglePointerById = new Map();
    const agendaSectionToggleIndex = new Map();
    const agendaSectionItemIndex = new Map();
    const agendaSectionActionIndex = new Map();
    let activeDoneConfirm = null;

    const closeDoneConfirm = (result = false) => {
      if (!activeDoneConfirm) return;
      const current = activeDoneConfirm;
      activeDoneConfirm = null;
      if (typeof current.cleanup === 'function') {
        current.cleanup();
      }
      current.resolve(Boolean(result));
    };

    const positionDoneConfirm = (node, anchor) => {
      if (!node) return;
      const margin = 8;
      const offset = 12;
      const width = Math.max(1, node.offsetWidth || 0);
      const height = Math.max(1, node.offsetHeight || 0);
      const viewportWidth = Math.max(1, window.innerWidth || document.documentElement.clientWidth || 0);
      const viewportHeight = Math.max(1, window.innerHeight || document.documentElement.clientHeight || 0);
      let left = Number(anchor && anchor.x) + offset;
      let top = Number(anchor && anchor.y) + offset;
      if (!Number.isFinite(left)) left = margin;
      if (!Number.isFinite(top)) top = margin;
      if ((left + width) > (viewportWidth - margin)) {
        left = Number(anchor && anchor.x) - width - offset;
      }
      if ((top + height) > (viewportHeight - margin)) {
        top = Number(anchor && anchor.y) - height - offset;
      }
      left = Math.max(margin, Math.min(left, viewportWidth - width - margin));
      top = Math.max(margin, Math.min(top, viewportHeight - height - margin));
      node.style.left = `${Math.round(left)}px`;
      node.style.top = `${Math.round(top)}px`;
    };

    const defaultToggleAnchor = (toggle) => {
      if (!toggle || typeof toggle.getBoundingClientRect !== 'function') {
        return { x: 24, y: 24 };
      }
      const rect = toggle.getBoundingClientRect();
      return {
        x: rect.left + (rect.width / 2),
        y: rect.top + (rect.height / 2),
      };
    };

    const consumeToggleAnchor = (toggleId, toggle) => {
      const stored = toggleId ? togglePointerById.get(toggleId) : null;
      if (toggleId) togglePointerById.delete(toggleId);
      if (stored && Number.isFinite(stored.x) && Number.isFinite(stored.y)) {
        return stored;
      }
      return defaultToggleAnchor(toggle);
    };

    const showDoneConfirm = (anchor, options = {}) => new Promise((resolve) => {
      closeDoneConfirm(false);
      const promptLabel = String(options.promptLabel || 'Mark Done?').trim() || 'Mark Done?';
      const confirmAriaLabel = String(options.confirmAriaLabel || 'Confirm status change').trim() || 'Confirm status change';
      const container = document.createElement('div');
      container.className = 'planner-done-confirm';
      container.setAttribute('role', 'dialog');
      container.setAttribute('aria-label', 'Confirm mark done');
      container.innerHTML = `
        <div class="planner-done-confirm__label">${promptLabel}</div>
        <div class="planner-done-confirm__actions">
          <button type="button" class="planner-done-confirm__btn planner-done-confirm__btn--confirm" data-done-confirm="yes" aria-label="${confirmAriaLabel}">✓</button>
          <button type="button" class="planner-done-confirm__btn planner-done-confirm__btn--cancel" data-done-confirm="no" aria-label="Cancel mark done">✕</button>
        </div>
      `;
      document.body.appendChild(container);
      positionDoneConfirm(container, anchor);
      window.requestAnimationFrame(() => {
        positionDoneConfirm(container, anchor);
      });

      const confirmButton = container.querySelector('[data-done-confirm="yes"]');
      const cancelButton = container.querySelector('[data-done-confirm="no"]');

      const onDocumentPointerDown = (event) => {
        const target = event.target;
        if (target && container.contains(target)) return;
        closeDoneConfirm(false);
      };
      const onKeyDown = (event) => {
        if (event.key !== 'Escape') return;
        event.preventDefault();
        closeDoneConfirm(false);
      };
      const onResize = () => {
        positionDoneConfirm(container, anchor);
      };

      const cleanup = () => {
        document.removeEventListener('pointerdown', onDocumentPointerDown, true);
        document.removeEventListener('keydown', onKeyDown);
        window.removeEventListener('resize', onResize);
        if (container.parentNode) {
          container.parentNode.removeChild(container);
        }
      };

      activeDoneConfirm = { resolve, cleanup };
      document.addEventListener('pointerdown', onDocumentPointerDown, true);
      document.addEventListener('keydown', onKeyDown);
      window.addEventListener('resize', onResize);

      if (confirmButton) {
        confirmButton.addEventListener('click', () => closeDoneConfirm(true));
        window.requestAnimationFrame(() => {
          confirmButton.focus();
        });
      }
      if (cancelButton) {
        cancelButton.addEventListener('click', () => closeDoneConfirm(false));
      }
    });

    const confirmDoneToggle = async (toggleId, toggle, previousDone, nextDone) => {
      if (Boolean(previousDone) === Boolean(nextDone)) return true;
      const anchor = consumeToggleAnchor(toggleId, toggle);
      if (Boolean(nextDone)) {
        return showDoneConfirm(anchor, {
          promptLabel: 'Mark Done?',
          confirmAriaLabel: 'Confirm mark done',
        });
      }
      return showDoneConfirm(anchor, {
        promptLabel: 'Mark Not Done?',
        confirmAriaLabel: 'Confirm mark not done',
      });
    };

    const mergeItems = () => {
      items = normalizeItems(
        [...localItems, ...remoteItems].sort((a, b) => `${a.date} ${a.time || '99:99'}`.localeCompare(`${b.date} ${b.time || '99:99'}`))
      );
    };

    const readLocalItems = () => {
      if (readOnly) return [];
      const storageKey = resolveStorageKey(root, options);
      if (!storageKey) return normalizeItems(seedItems());

      const raw = readStorage(storageKey);
      if (!raw) {
        const seeded = normalizeItems(seedItems());
        writeStorage(storageKey, JSON.stringify(seeded));
        return seeded;
      }

      try {
        const parsed = JSON.parse(raw);
        return normalizeItems(parsed);
      } catch (error) {
        const fallback = normalizeItems(seedItems());
        writeStorage(storageKey, JSON.stringify(fallback));
        return fallback;
      }
    };

    const readExternalItems = () => {
      try {
        return normalizeItems(externalItems(), { isExternal: true, source: 'external' });
      } catch (error) {
        return [];
      }
    };

    const persistLocalItems = () => {
      if (readOnly) return;
      const storageKey = resolveStorageKey(root, options);
      if (!storageKey) return;
      writeStorage(storageKey, JSON.stringify(localItems));
    };

    const updateItemDoneState = (itemId, done) => {
      let localUpdated = false;
      localItems = localItems.map((item) => {
        if (item.id !== itemId) return item;
        localUpdated = true;
        return { ...item, done: Boolean(done) };
      });
      if (localUpdated) {
        persistLocalItems();
      } else {
        remoteItems = remoteItems.map((item) => (item.id === itemId ? { ...item, done: Boolean(done) } : item));
      }
      mergeItems();
    };

    const findItemById = (itemId) => items.find((item) => item.id === itemId) || null;

    const applyExternalToggle = async (item, nextDone) => {
      if (!item || !item.isExternal) return;
      if (!onExternalToggle) return;
      await onExternalToggle(item, Boolean(nextDone));
    };

    const setDateInput = () => {
      if (dateInput) dateInput.value = selectedDate;
    };

    const filteredItems = () => {
      if (activeFilter === 'all') return items;
      return items.filter((item) => item.kind === activeFilter);
    };

    const sectionToggleKey = (sectionId, itemId) => `section:${String(sectionId || '').trim()}:${String(itemId || '').trim()}`;

    const showTimelineItemsForView = (view) => {
      const normalizedView = String(view || '').trim().toLowerCase();
      const option = (
        normalizedView === 'tasks'
        || normalizedView === 'all'
        || normalizedView === 'agenda'
        || normalizedView === 'month-list'
      )
        ? showListTimelineItemsOption
        : showMonthTimelineItemsOption;
      if (typeof option === 'boolean') return option;
      if (typeof option === 'function') {
        try {
          return Boolean(
            option({
              view: normalizedView,
              selectedDate,
              activeFilter,
              items,
              filteredItems: filteredItems(),
            })
          );
        } catch (error) {
          return true;
        }
      }
      return true;
    };

    const resolveAgendaSections = (view) => {
      try {
        return normalizeAgendaSections(
          agendaSections({
            view,
            selectedDate,
            activeFilter,
            items,
            filteredItems: filteredItems(),
            calendarCursor: toYmd(calendarCursor),
          })
        );
      } catch (error) {
        return [];
      }
    };

    const formatSourceLabel = (source) => {
      const normalized = String(source || '').trim().toLowerCase();
      if (!normalized) return '';
      if (normalized === 'asana') return 'Asana';
      if (normalized === 'outlook') return 'Outlook';
      return `${normalized.charAt(0).toUpperCase()}${normalized.slice(1)}`;
    };

    const itemKindCount = (kind) => {
      if (kind === 'all') return items.filter((item) => !item.done).length;
      return items.filter((item) => item.kind === kind && !item.done).length;
    };

    const updateStats = () => {
      const todayKey = toYmd(new Date());
      const endOfWeek = addDays(parseYmd(todayKey), 6);
      const weekEndKey = endOfWeek ? toYmd(endOfWeek) : todayKey;
      const openItems = items.filter((item) => !item.done);
      const todayCount = openItems.filter((item) => item.date === todayKey).length;
      const weekCount = openItems.filter((item) => item.date >= todayKey && item.date <= weekEndKey).length;
      const doneCount = items.filter((item) => item.done).length;

      if (statNodes.today) statNodes.today.textContent = String(todayCount);
      if (statNodes.week) statNodes.week.textContent = String(weekCount);
      if (statNodes.done) statNodes.done.textContent = String(doneCount);
    };

    const updateFilterButtons = () => {
      filterButtons.forEach((button) => {
        const kind = String(button.getAttribute('data-planner-filter') || '').trim().toLowerCase();
        button.classList.toggle('is-active', kind === activeFilter);
        const countNode = button.querySelector('[data-planner-filter-count]');
        if (countNode) countNode.textContent = String(itemKindCount(kind || 'all'));
      });
    };

    const updateViewButtons = () => {
      viewButtons.forEach((button) => {
        const view = String(button.getAttribute('data-planner-view') || '').trim().toLowerCase();
        button.classList.toggle('is-active', view === activeView);
      });
      root.classList.remove('planner-panel--agenda');
    };

    const buildAgendaItem = (item) => {
      const row = document.createElement('label');
      row.className = 'planner-item';
      if (item.done) row.classList.add('is-done');

      const tick = document.createElement('input');
      tick.type = 'checkbox';
      tick.checked = Boolean(item.done);
      tick.disabled = !item.canToggle || togglePendingById.has(item.id) || (item.isExternal && item.canToggle && !onExternalToggle);
      tick.setAttribute('data-planner-toggle', item.id);
      tick.setAttribute('aria-label', `Mark ${item.title} as complete`);

      const body = document.createElement('div');
      body.className = 'planner-item__body';

      const titleWrap = document.createElement('strong');
      titleWrap.className = 'planner-item__title';
      if (onItemClick) {
        const titleBtn = document.createElement('button');
        titleBtn.type = 'button';
        titleBtn.className = 'planner-item__title-btn';
        titleBtn.textContent = item.title;
        titleBtn.addEventListener('click', (event) => {
          event.preventDefault();
          event.stopPropagation();
          onItemClick(item);
        });
        titleWrap.appendChild(titleBtn);
        if (item.url) {
          const extLink = document.createElement('a');
          extLink.href = item.url;
          extLink.target = '_blank';
          extLink.rel = 'noopener noreferrer';
          extLink.className = 'planner-item__ext-link';
          extLink.title = 'Open in Asana';
          extLink.textContent = '↗';
          titleWrap.appendChild(extLink);
        }
      } else if (item.url) {
        const link = document.createElement('a');
        link.href = item.url;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        link.className = 'overview-resource-link';
        link.textContent = item.title;
        titleWrap.appendChild(link);
      } else {
        titleWrap.textContent = item.title;
      }

      const meta = document.createElement('div');
      meta.className = 'planner-item__meta';

      const when = document.createElement('span');
      when.className = 'planner-item__time';
      when.textContent = `${prettyDay(item.date)}${item.time ? ` at ${prettyTime(item.time)}` : ''}`;

      const kind = document.createElement('span');
      kind.className = `planner-kind-pill planner-kind-pill--${item.kind}`;
      kind.textContent = KIND_LABELS[item.kind] || KIND_LABELS['follow-up'];

      meta.appendChild(when);
      meta.appendChild(kind);
      body.appendChild(titleWrap);
      body.appendChild(meta);
      row.appendChild(tick);
      row.appendChild(body);
      return row;
    };

    const buildAgendaSectionItem = (section, item) => {
      const canToggle = Boolean(item.canToggle);
      const key = sectionToggleKey(section.id, item.id);
      const row = document.createElement(canToggle ? 'label' : 'div');
      row.className = 'planner-item planner-item--section';
      if (item.done) row.classList.add('is-done');
      agendaSectionItemIndex.set(key, { section, item });

      if (canToggle) {
        const tick = document.createElement('input');
        tick.type = 'checkbox';
        tick.checked = Boolean(item.done);
        tick.disabled = togglePendingById.has(key) || (item.isExternal && !onExternalToggle);
        tick.setAttribute('data-planner-section-toggle', key);
        tick.setAttribute('aria-label', `Mark ${item.title} as complete`);
        row.appendChild(tick);
        agendaSectionToggleIndex.set(key, { section, item });
      }

      const body = document.createElement('div');
      body.className = 'planner-item__body';

      const titleWrap = document.createElement('strong');
      titleWrap.className = 'planner-item__title';
      if (onItemClick) {
        const titleBtn = document.createElement('button');
        titleBtn.type = 'button';
        titleBtn.className = 'planner-item__title-btn';
        titleBtn.textContent = item.title;
        titleBtn.addEventListener('click', (event) => {
          event.preventDefault();
          event.stopPropagation();
          onItemClick(item);
        });
        titleWrap.appendChild(titleBtn);
        if (item.url) {
          const extLink = document.createElement('a');
          extLink.href = item.url;
          extLink.target = '_blank';
          extLink.rel = 'noopener noreferrer';
          extLink.className = 'planner-item__ext-link';
          extLink.title = 'Open in Asana';
          extLink.textContent = '↗';
          titleWrap.appendChild(extLink);
        }
      } else if (item.url) {
        const link = document.createElement('a');
        link.href = item.url;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        link.className = 'overview-resource-link';
        link.textContent = item.title;
        titleWrap.appendChild(link);
      } else {
        titleWrap.textContent = item.title;
      }

      const meta = document.createElement('div');
      meta.className = 'planner-item__meta';

      if (item.meta) {
        const metaText = document.createElement('span');
        metaText.className = 'planner-item__time';
        metaText.textContent = item.meta;
        meta.appendChild(metaText);
      }

      if (item.source) {
        const source = document.createElement('span');
        source.className = 'planner-item__source';
        source.textContent = formatSourceLabel(item.source);
        meta.appendChild(source);
      }

      const head = document.createElement('div');
      head.className = 'planner-item__head';
      head.appendChild(titleWrap);
      if (Array.isArray(item.badges) && item.badges.length) {
        const badgesWrap = document.createElement('div');
        badgesWrap.className = 'planner-item__badges';
        item.badges.forEach((badge) => {
          const badgeButton = document.createElement('button');
          badgeButton.type = 'button';
          badgeButton.className = 'planner-item-badge-btn';
          badgeButton.setAttribute('data-planner-section-action', badge.id);
          badgeButton.setAttribute('data-planner-section-item-key', key);
          badgeButton.textContent = badge.label;
          if (badge.title) {
            badgeButton.title = badge.title;
            badgeButton.setAttribute('aria-label', badge.title);
          }
          badgeButton.addEventListener('click', (event) => {
            event.preventDefault();
          });
          badgesWrap.appendChild(badgeButton);
        });
        head.appendChild(badgesWrap);
      }

      body.appendChild(head);
      if (Array.isArray(item.actions) && item.actions.length) {
        const actionsWrap = document.createElement('div');
        actionsWrap.className = 'planner-item__actions';
        item.actions.forEach((action) => {
          const actionButton = document.createElement('button');
          actionButton.type = 'button';
          actionButton.className = 'planner-item-action-btn';
          actionButton.setAttribute('data-planner-section-action', action.id);
          actionButton.setAttribute('data-planner-section-item-key', key);
          actionButton.textContent = action.label;
          actionButton.addEventListener('click', (event) => {
            event.preventDefault();
          });
          actionsWrap.appendChild(actionButton);
        });
        body.appendChild(actionsWrap);
      }
      if (meta.childNodes.length > 0) {
        body.appendChild(meta);
      }
      if (item.inlineComment && onInlineComment) {
        const commentStrip = document.createElement('div');
        commentStrip.className = 'planner-item__inline-comment';

        [
          { label: '🎉', text: '🎉' },
          { label: '👍', text: '👍' },
          { label: '✅', text: '✅' },
          { label: 'Awesome!', text: 'Awesome!' },
          { label: 'Great work!', text: 'Great work!' },
        ].forEach(({ label, text }) => {
          const btn = document.createElement('button');
          btn.type = 'button';
          btn.className = 'planner-item__react-btn';
          btn.textContent = label;
          btn.setAttribute('aria-label', `React: ${text}`);
          btn.addEventListener('click', async (event) => {
            event.preventDefault();
            event.stopPropagation();
            btn.disabled = true;
            try {
              await onInlineComment(item, text);
              btn.classList.add('is-sent');
              setTimeout(() => { btn.classList.remove('is-sent'); btn.disabled = false; }, 1200);
            } catch (error) {
              btn.disabled = false;
            }
          });
          commentStrip.appendChild(btn);
        });

        const textInput = document.createElement('input');
        textInput.type = 'text';
        textInput.className = 'planner-item__comment-input';
        textInput.placeholder = 'Comment…';
        textInput.maxLength = 500;
        textInput.addEventListener('click', (event) => { event.stopPropagation(); });

        const sendBtn = document.createElement('button');
        sendBtn.type = 'button';
        sendBtn.className = 'planner-item__comment-send';
        sendBtn.textContent = '↵';
        sendBtn.setAttribute('aria-label', 'Send comment');

        const doSend = async (event) => {
          event.preventDefault();
          event.stopPropagation();
          const text = String(textInput.value || '').trim();
          if (!text) return;
          sendBtn.disabled = true;
          try {
            await onInlineComment(item, text);
            textInput.value = '';
            sendBtn.classList.add('is-sent');
            setTimeout(() => { sendBtn.classList.remove('is-sent'); sendBtn.disabled = false; }, 1200);
          } catch (error) {
            sendBtn.disabled = false;
          }
        };

        sendBtn.addEventListener('click', doSend);
        textInput.addEventListener('keydown', (event) => {
          if (event.key === 'Enter' && !event.shiftKey) {
            doSend(event);
          }
        });

        commentStrip.appendChild(textInput);
        commentStrip.appendChild(sendBtn);
        body.appendChild(commentStrip);
      }
      row.appendChild(body);
      return row;
    };

    const appendAgendaSections = (sections) => {
      agendaSectionToggleIndex.clear();
      agendaSectionItemIndex.clear();
      agendaSectionActionIndex.clear();
      sections.forEach((section) => {
        const divider = document.createElement('div');
        divider.className = 'planner-agenda-divider';

        const textWrap = document.createElement('div');
        textWrap.className = 'planner-agenda-divider__text';

        const title = document.createElement('strong');
        title.textContent = section.title;
        textWrap.appendChild(title);

        if (section.description) {
          const description = document.createElement('span');
          description.className = 'text-muted small';
          description.textContent = section.description;
          textWrap.appendChild(description);
        }
        divider.appendChild(textWrap);

        if (Array.isArray(section.actions) && section.actions.length) {
          const actionsWrap = document.createElement('div');
          actionsWrap.className = 'planner-agenda-divider__actions';
          section.actions.forEach((action) => {
            const actionKey = `${section.id}:${action.id}`;
            agendaSectionActionIndex.set(actionKey, { section, action });

            const button = document.createElement('button');
            button.type = 'button';
            button.className = 'planner-section-action-btn';
            button.setAttribute('data-planner-section-header-action', action.id);
            button.setAttribute('data-planner-section-id', section.id);
            button.textContent = action.label;
            if (action.title) {
              button.title = action.title;
              button.setAttribute('aria-label', action.title);
            }
            if (action.iconOnly) {
              button.classList.add('planner-section-action-btn--icon');
            }
            actionsWrap.appendChild(button);
          });
          divider.appendChild(actionsWrap);
        }

        const body = document.createElement('div');
        body.className = 'planner-agenda-section-items';
        if (Array.isArray(section.items) && section.items.length) {
          section.items.forEach((item) => {
            body.appendChild(buildAgendaSectionItem(section, item));
          });
        } else {
          const empty = document.createElement('p');
          empty.className = 'planner-empty text-muted small';
          empty.textContent = section.emptyText || 'No items in this section.';
          body.appendChild(empty);
        }

        agenda.appendChild(divider);
        agenda.appendChild(body);
      });
    };

    const applyAgendaSectionToggle = async (item, nextDone) => {
      if (!item || !item.canToggle) return;
      if (item.isExternal) {
        await applyExternalToggle(item, nextDone);
      }
    };

    const renderAgendaEmpty = (message) => {
      agendaSectionToggleIndex.clear();
      agendaSectionItemIndex.clear();
      agendaSectionActionIndex.clear();
      while (agenda.firstChild) agenda.removeChild(agenda.firstChild);
      const empty = document.createElement('p');
      empty.className = 'planner-empty text-muted small';
      empty.textContent = message;
      agenda.appendChild(empty);
    };

    const renderAgendaMonth = () => {
      if (agendaTitle) agendaTitle.textContent = 'Tasks';
      const startDate = parseYmd(selectedDate) || new Date();
      const endDate = addDays(startDate, 13) || startDate;
      const startKey = toYmd(startDate);
      const endKey = toYmd(endDate);
      selectedLabel.textContent = `${prettyDay(startKey)} to ${prettyDay(endKey)}`;

      const dayItems = showTimelineItemsForView('tasks')
        ? filteredItems()
          .filter((item) => item.date >= startKey && item.date <= endKey)
          .sort((a, b) => `${a.date} ${a.time || '99:99'}`.localeCompare(`${b.date} ${b.time || '99:99'}`))
        : [];
      const extraSections = resolveAgendaSections('tasks');

      if (!dayItems.length && !extraSections.length) {
        renderAgendaEmpty(monthEmptyText);
        return;
      }

      while (agenda.firstChild) agenda.removeChild(agenda.firstChild);
      if (extraSections.length) {
        appendAgendaSections(extraSections);
      } else {
        agendaSectionToggleIndex.clear();
        agendaSectionItemIndex.clear();
        agendaSectionActionIndex.clear();
      }
      dayItems.forEach((item) => {
        agenda.appendChild(buildAgendaItem(item));
      });
    };

    const renderAgendaList = () => {
      const historyStartDate = addDays(new Date(), -(listHistoryDays - 1)) || new Date();
      const historyStartKey = toYmd(historyStartDate);

      if (agendaTitle) agendaTitle.textContent = 'Tasks';
      selectedLabel.textContent = `Last ${listHistoryDays} days + upcoming`;

      const timelineItems = showTimelineItemsForView('all')
        ? filteredItems()
          .filter((item) => item.date >= historyStartKey)
          .sort((a, b) => `${a.date} ${a.time || '99:99'}`.localeCompare(`${b.date} ${b.time || '99:99'}`))
        : [];
      const extraSections = resolveAgendaSections('all');

      if (!timelineItems.length && !extraSections.length) {
        renderAgendaEmpty(listEmptyText);
        return;
      }

      while (agenda.firstChild) agenda.removeChild(agenda.firstChild);
      if (extraSections.length) {
        appendAgendaSections(extraSections);
      } else {
        agendaSectionToggleIndex.clear();
        agendaSectionItemIndex.clear();
        agendaSectionActionIndex.clear();
      }
      let currentDateKey = '';
      let group = null;
      let groupBody = null;

      timelineItems.forEach((item) => {
        if (item.date !== currentDateKey) {
          currentDateKey = item.date;
          group = document.createElement('section');
          group.className = 'planner-agenda-group';

          const head = document.createElement('header');
          head.className = 'planner-agenda-group__head';

          const dateLabel = document.createElement('strong');
          dateLabel.textContent = prettyDay(item.date);
          head.appendChild(dateLabel);

          const count = timelineItems.filter((entry) => entry.date === item.date).length;
          const countLabel = document.createElement('span');
          countLabel.className = 'text-muted small';
          countLabel.textContent = `${count} item${count === 1 ? '' : 's'}`;
          head.appendChild(countLabel);

          groupBody = document.createElement('div');
          groupBody.className = 'planner-agenda-group__items';
          group.appendChild(head);
          group.appendChild(groupBody);
          agenda.appendChild(group);
        }
        if (groupBody) groupBody.appendChild(buildAgendaItem(item));
      });
    };

    const renderCalendar = () => {
      monthLabel.textContent = prettyMonth(calendarCursor);
      while (grid.firstChild) grid.removeChild(grid.firstChild);

      WEEKDAY_LABELS.forEach((label) => {
        const node = document.createElement('div');
        node.className = 'planner-weekday';
        node.textContent = label;
        grid.appendChild(node);
      });

      const monthStart = monthFloor(calendarCursor);
      const gridStart = addDays(monthStart, -monthStart.getDay()) || monthStart;
      const todayKey = toYmd(new Date());
      const visibleItems = filteredItems();

      for (let idx = 0; idx < 42; idx += 1) {
        const day = addDays(gridStart, idx);
        if (!day) continue;
        const dayKey = toYmd(day);
        const dayItems = visibleItems.filter((item) => item.date === dayKey);
        const openCount = dayItems.filter((item) => !item.done).length;
        const dayKinds = Array.from(new Set(dayItems.filter((item) => !item.done).map((item) => item.kind))).slice(0, 3);

        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'planner-day';
        button.setAttribute('data-day-key', dayKey);
        if (day.getMonth() !== monthStart.getMonth()) button.classList.add('is-outside');
        if (dayKey === todayKey) button.classList.add('is-today');
        if (dayKey === selectedDate) button.classList.add('is-selected');
        if (openCount > 0) button.classList.add('has-items');

        const top = document.createElement('div');
        top.className = 'planner-day__top';

        const num = document.createElement('span');
        num.className = 'planner-day__num';
        num.textContent = String(day.getDate());
        top.appendChild(num);

        if (openCount > 0) {
          const badge = document.createElement('span');
          badge.className = 'planner-day__count';
          badge.textContent = String(openCount);
          top.appendChild(badge);
        }

        button.appendChild(top);

        const dots = document.createElement('div');
        dots.className = 'planner-day__dots';
        dayKinds.forEach((kind) => {
          const dot = document.createElement('span');
          dot.className = `planner-day-dot planner-day-dot--${kind}`;
          dot.setAttribute('aria-hidden', 'true');
          dots.appendChild(dot);
        });
        button.appendChild(dots);
        grid.appendChild(button);
      }
    };

    const renderAgenda = () => {
      if (activeView === 'all') {
        renderAgendaList();
      } else {
        renderAgendaMonth();
      }
    };

    const render = () => {
      updateStats();
      updateFilterButtons();
      updateViewButtons();
      renderCalendar();
      renderAgenda();
      setDateInput();
    };

    if (grid) {
      grid.addEventListener('click', (event) => {
        const target = event.target.closest('[data-day-key]');
        if (!target) return;
        selectedDate = String(target.getAttribute('data-day-key') || '').trim() || selectedDate;
        render();
      });
    }

    if (agenda) {
      agenda.addEventListener('pointerdown', (event) => {
        const target = event.target;
        let toggle = target && typeof target.closest === 'function'
          ? target.closest('[data-planner-toggle], [data-planner-section-toggle]')
          : null;
        if (!toggle && target && typeof target.closest === 'function') {
          const row = target.closest('label.planner-item');
          if (row) {
            toggle = row.querySelector('[data-planner-toggle], [data-planner-section-toggle]');
          }
        }
        if (!toggle) return;
        const toggleId = String(
          toggle.getAttribute('data-planner-section-toggle')
          || toggle.getAttribute('data-planner-toggle')
          || ''
        ).trim();
        if (!toggleId) return;
        const x = Number(event.clientX);
        const y = Number(event.clientY);
        if (!Number.isFinite(x) || !Number.isFinite(y)) return;
        togglePointerById.set(toggleId, { x, y });
      });

      agenda.addEventListener('click', async (event) => {
        const sectionActionButton = event.target.closest('[data-planner-section-header-action]');
        if (sectionActionButton) {
          if (!onAgendaSectionAction) return;
          const actionId = String(sectionActionButton.getAttribute('data-planner-section-header-action') || '').trim();
          const sectionId = String(sectionActionButton.getAttribute('data-planner-section-id') || '').trim();
          if (!actionId || !sectionId) return;
          const entry = agendaSectionActionIndex.get(`${sectionId}:${actionId}`);
          if (!entry || !entry.section) return;
          try {
            await onAgendaSectionAction(null, actionId, entry.section);
          } catch (error) {
            if (onActionError) {
              onActionError(null, actionId, error);
            }
          }
          return;
        }

        const actionButton = event.target.closest('[data-planner-section-action]');
        if (!actionButton) return;
        if (!onAgendaSectionAction) return;
        const actionId = String(actionButton.getAttribute('data-planner-section-action') || '').trim();
        const itemKey = String(actionButton.getAttribute('data-planner-section-item-key') || '').trim();
        if (!actionId || !itemKey) return;
        const entry = agendaSectionItemIndex.get(itemKey);
        if (!entry || !entry.item) return;
        try {
          await onAgendaSectionAction(entry.item, actionId, entry.section);
        } catch (error) {
          if (onActionError) {
            onActionError(entry.item, actionId, error);
          }
        }
      });

      agenda.addEventListener('change', async (event) => {
        if (readOnly) {
          const toggle = event.target;
          if (toggle && toggle.matches('input[type="checkbox"]')) {
            const currentlyChecked = Boolean(toggle.checked);
            toggle.checked = !currentlyChecked;
          }
          return;
        }
        const toggle = event.target;
        if (!toggle) return;
        if (toggle.matches('[data-planner-section-toggle]')) {
          const sectionKey = String(toggle.getAttribute('data-planner-section-toggle') || '').trim();
          if (!sectionKey) return;
          if (togglePendingById.has(sectionKey)) return;
          const entry = agendaSectionToggleIndex.get(sectionKey);
          if (!entry || !entry.item) return;
          const nextDone = Boolean(toggle.checked);
          const previousDone = Boolean(entry.item.done);
          const confirmed = await confirmDoneToggle(sectionKey, toggle, previousDone, nextDone);
          if (!confirmed) {
            toggle.checked = previousDone;
            return;
          }

          togglePendingById.add(sectionKey);
          entry.item.done = nextDone;
          render();

          try {
            await applyAgendaSectionToggle(entry.item, nextDone);
          } catch (error) {
            entry.item.done = previousDone;
            if (onToggleError) {
              onToggleError(entry.item, error);
            }
          } finally {
            if (entry.item.isExternal) {
              remoteItems = readExternalItems();
              mergeItems();
            }
            togglePendingById.delete(sectionKey);
            render();
          }
          return;
        }
        if (!toggle.matches('[data-planner-toggle]')) return;
        const itemId = String(toggle.getAttribute('data-planner-toggle') || '').trim();
        if (!itemId) return;
        if (togglePendingById.has(itemId)) return;
        const item = findItemById(itemId);
        if (!item) return;
        if (!item.canToggle) {
          toggle.checked = Boolean(item.done);
          return;
        }
        const nextDone = Boolean(toggle.checked);
        const previousDone = Boolean(item.done);
        const confirmed = await confirmDoneToggle(itemId, toggle, previousDone, nextDone);
        if (!confirmed) {
          toggle.checked = previousDone;
          return;
        }

        togglePendingById.add(itemId);
        updateItemDoneState(itemId, nextDone);
        render();

        if (item.isExternal) {
          try {
            await applyExternalToggle(item, nextDone);
          } catch (error) {
            updateItemDoneState(itemId, previousDone);
            if (onToggleError) {
              onToggleError(item, error);
            }
          } finally {
            togglePendingById.delete(itemId);
            render();
          }
          return;
        }

        togglePendingById.delete(itemId);
        render();
      });
    }

    if (prevButton) {
      prevButton.addEventListener('click', () => {
        calendarCursor = monthFloor(new Date(calendarCursor.getFullYear(), calendarCursor.getMonth() - 1, 1));
        render();
      });
    }

    if (nextButton) {
      nextButton.addEventListener('click', () => {
        calendarCursor = monthFloor(new Date(calendarCursor.getFullYear(), calendarCursor.getMonth() + 1, 1));
        render();
      });
    }

    if (todayButton) {
      todayButton.addEventListener('click', () => {
        calendarCursor = monthFloor(new Date());
        render();
      });
    }

    if (addForm && readOnly) {
      addForm.classList.add('d-none');
    }

    if (addForm && titleInput && dateInput && kindInput && !readOnly) {
      addForm.addEventListener('submit', (event) => {
        event.preventDefault();
        const title = String(titleInput.value || '').trim();
        const date = String(dateInput.value || '').trim();
        if (!title || !parseYmd(date)) return;

        localItems.push({
          id: createId(),
          title,
          date,
          time: timeInput ? String(timeInput.value || '').trim() : '',
          kind: normalizeKind(kindInput.value || 'follow-up'),
          done: false,
          source: 'local',
          isExternal: false,
        });
        localItems = normalizeItems(localItems, { source: 'local' });
        persistLocalItems();
        mergeItems();

        titleInput.value = '';
        if (timeInput) timeInput.value = '';
        selectedDate = date;
        calendarCursor = monthFloor(date);
        activeView = 'tasks';
        render();
        titleInput.focus();
      });
    }

    filterButtons.forEach((button) => {
      button.addEventListener('click', () => {
        const nextFilter = String(button.getAttribute('data-planner-filter') || '').trim().toLowerCase();
        activeFilter = nextFilter || 'all';
        render();
      });
    });

    viewButtons.forEach((button) => {
      button.addEventListener('click', () => {
        const nextView = String(button.getAttribute('data-planner-view') || '').trim().toLowerCase();
        if (nextView === 'all' || nextView === 'month-list') activeView = 'all';
        else activeView = 'tasks';
        render();
      });
    });

    const reload = (reloadOptions = {}) => {
      localItems = readLocalItems();
      remoteItems = readExternalItems();
      mergeItems();
      if (reloadOptions.resetDate) {
        selectedDate = toYmd(new Date());
        calendarCursor = monthFloor(new Date());
      } else {
        const selected = parseYmd(selectedDate);
        if (!selected) selectedDate = toYmd(new Date());
        calendarCursor = parseYmd(selectedDate) ? monthFloor(selectedDate) : monthFloor(new Date());
      }
      if (reloadOptions.resetView) {
        activeView = 'tasks';
        activeFilter = 'all';
      }
      render();
    };

    localItems = readLocalItems();
    remoteItems = readExternalItems();
    mergeItems();
    setDateInput();
    render();

    const controller = {
      reload,
      setExternalItems: (nextItems) => {
        remoteItems = normalizeItems(Array.isArray(nextItems) ? nextItems : [], { isExternal: true, source: 'external' });
        mergeItems();
        render();
      },
      setDate: (value) => {
        const normalized = toYmd(value);
        if (!normalized) return;
        selectedDate = normalized;
        calendarCursor = monthFloor(normalized);
        render();
      },
    };

    root.__plannerController = controller;
    return controller;
  };

  window.AlshivalPlanner = {
    init: initPlanner,
  };
})();
