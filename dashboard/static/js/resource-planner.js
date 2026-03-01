(() => {
  const plannerRoot = document.querySelector('[data-resource-planner]');
  if (!plannerRoot) {
    return;
  }

  const getCookie = (name) => {
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
  };

  const parseJsonNode = (node, fallback) => {
    if (!node) return fallback;
    try {
      const parsed = JSON.parse(node.textContent || '');
      if (Array.isArray(fallback)) return Array.isArray(parsed) ? parsed : fallback;
      if (fallback && typeof fallback === 'object') return (parsed && typeof parsed === 'object') ? parsed : fallback;
      return parsed;
    } catch (error) {
      return fallback;
    }
  };

  const normalizeUuid = (value) => String(value || '').trim().toLowerCase();

  const normalizeUuidList = (values) => {
    if (!Array.isArray(values)) return [];
    const normalized = [];
    const seen = new Set();
    values.forEach((raw) => {
      const candidate = normalizeUuid(raw);
      if (!candidate || seen.has(candidate)) return;
      seen.add(candidate);
      normalized.push(candidate);
    });
    return normalized;
  };

  const normalizeMappingMap = (raw) => {
    const normalized = {};
    if (!raw || typeof raw !== 'object') return normalized;
    Object.keys(raw).forEach((key) => {
      const mapKey = String(key || '').trim();
      if (!mapKey) return;
      normalized[mapKey] = normalizeUuidList(raw[key]);
    });
    return normalized;
  };

  const normalizeResourceOptions = (rawOptions) => {
    if (!Array.isArray(rawOptions)) return [];
    const normalized = [];
    const seen = new Set();
    rawOptions.forEach((option) => {
      if (!option || typeof option !== 'object') return;
      const resourceUuid = normalizeUuid(option.resource_uuid);
      const resourceName = String(option.resource_name || '').trim();
      if (!resourceUuid || !resourceName || seen.has(resourceUuid)) return;
      seen.add(resourceUuid);
      normalized.push({
        resource_uuid: resourceUuid,
        resource_name: resourceName,
      });
    });
    return normalized.sort((a, b) => a.resource_name.localeCompare(b.resource_name));
  };

  const asanaBoardRowsFromTaskRaw = (taskRow) => {
    const links = Array.isArray(taskRow && taskRow.project_links) ? taskRow.project_links : [];
    const workspaceGid = String(taskRow && taskRow.workspace_gid ? taskRow.workspace_gid : '').trim();
    const workspaceName = String(taskRow && taskRow.workspace_name ? taskRow.workspace_name : '').trim();
    const rows = [];
    const seen = new Set();
    links.forEach((link) => {
      if (!link || typeof link !== 'object') return;
      const gid = String(link.gid || '').trim();
      const name = String(link.name || '').trim();
      if (!gid || !name || seen.has(gid)) return;
      seen.add(gid);
      rows.push({
        gid,
        name,
        url: String(link.url || '').trim(),
        workspace_gid: workspaceGid,
        workspace_name: workspaceName,
      });
    });
    return rows;
  };

  const normalizeBoardRows = (rawBoards) => {
    if (!Array.isArray(rawBoards)) return [];
    const normalized = [];
    const seen = new Set();
    rawBoards.forEach((board) => {
      if (!board || typeof board !== 'object') return;
      const gid = String(board.gid || '').trim();
      const name = String(board.name || '').trim();
      if (!gid || !name || seen.has(gid)) return;
      seen.add(gid);
      normalized.push({
        gid,
        name,
        url: String(board.url || '').trim(),
        workspace_gid: String(board.workspace_gid || board.workspaceGid || '').trim(),
        workspace_name: String(board.workspace_name || board.workspaceName || '').trim(),
      });
    });
    return normalized;
  };

  const mergeBoardRows = (...boardLists) => {
    const mergedByGid = new Map();
    boardLists.forEach((boardRows) => {
      if (!Array.isArray(boardRows)) return;
      boardRows.forEach((rawBoard) => {
        if (!rawBoard || typeof rawBoard !== 'object') return;
        const gid = String(rawBoard.gid || '').trim();
        const name = String(rawBoard.name || '').trim();
        if (!gid || !name) return;
        const normalized = {
          gid,
          name,
          url: String(rawBoard.url || '').trim(),
          workspace_gid: String(rawBoard.workspace_gid || rawBoard.workspaceGid || '').trim(),
          workspace_name: String(rawBoard.workspace_name || rawBoard.workspaceName || '').trim(),
        };
        const existing = mergedByGid.get(gid);
        if (!existing) {
          mergedByGid.set(gid, normalized);
          return;
        }
        if (!existing.url && normalized.url) existing.url = normalized.url;
        if (!existing.workspace_gid && normalized.workspace_gid) existing.workspace_gid = normalized.workspace_gid;
        if (!existing.workspace_name && normalized.workspace_name) existing.workspace_name = normalized.workspace_name;
      });
    });
    return Array.from(mergedByGid.values()).sort((left, right) => {
      const leftWorkspace = String(left.workspace_name || '').trim().toLowerCase();
      const rightWorkspace = String(right.workspace_name || '').trim().toLowerCase();
      const workspaceCompare = leftWorkspace.localeCompare(rightWorkspace);
      if (workspaceCompare !== 0) return workspaceCompare;
      const leftName = String(left.name || '').trim().toLowerCase();
      const rightName = String(right.name || '').trim().toLowerCase();
      return leftName.localeCompare(rightName);
    });
  };

  const toEpoch = (value) => {
    const resolved = String(value || '').trim();
    if (!resolved) return 0;
    const parsed = Date.parse(resolved);
    return Number.isFinite(parsed) ? parsed : 0;
  };

  const formatWhen = (value) => {
    const epoch = toEpoch(value);
    if (!epoch) return '';
    const date = new Date(epoch);
    try {
      return new Intl.DateTimeFormat([], {
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
      }).format(date);
    } catch (error) {
      return date.toLocaleString();
    }
  };

  const toYmdLocal = (value) => {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
  };

  const addDaysToYmd = (ymd, days) => {
    const base = String(ymd || '').trim();
    if (!base) return '';
    const date = new Date(`${base}T00:00:00`);
    if (Number.isNaN(date.getTime())) return '';
    date.setDate(date.getDate() + Number(days || 0));
    return toYmdLocal(date);
  };

  const resourceUuid = normalizeUuid(plannerRoot.getAttribute('data-resource-uuid') || '');
  const resourceId = String(plannerRoot.getAttribute('data-resource-planner-id') || '').trim();
  const isSuperuser = plannerRoot.getAttribute('data-superuser') === '1';
  const plannerStorageKey = `resource_planner_items_${resourceId || 'default'}`;
  const completedWindowDays = Math.max(
    1,
    Math.min(90, Number.parseInt(String(plannerRoot.getAttribute('data-asana-completed-window-days') || '30'), 10) || 30)
  );

  const asanaCompleteUrlTemplate = plannerRoot.getAttribute('data-asana-complete-url-template') || '';
  const asanaCreateTaskUrlTemplate = plannerRoot.getAttribute('data-asana-board-task-create-url-template') || '';
  const asanaDeleteTaskUrlTemplate = plannerRoot.getAttribute('data-asana-task-delete-url-template') || '';
  const asanaCommentsUrlTemplate = plannerRoot.getAttribute('data-asana-comments-url-template') || '';
  const asanaCommentAddUrlTemplate = plannerRoot.getAttribute('data-asana-comment-add-url-template') || '';
  const asanaBoardMapUrlTemplate = plannerRoot.getAttribute('data-asana-board-map-url-template') || '';
  const asanaTaskMapUrlTemplate = plannerRoot.getAttribute('data-asana-task-map-url-template') || '';
  const asanaWorkspaceMembersUrlTemplate = plannerRoot.getAttribute('data-asana-workspace-members-url-template') || '';

  const asanaTasksNode = document.getElementById('resource-asana-tasks');
  const asanaBoardsNode = document.getElementById('resource-asana-boards');
  const resourceOptionsNode = document.getElementById('resource-asana-resource-options');
  const boardMappingsNode = document.getElementById('resource-asana-board-resource-mappings');
  const taskMappingsNode = document.getElementById('resource-asana-task-resource-mappings');

  let asanaTasks = parseJsonNode(asanaTasksNode, []);
  const boardRowsFromTasks = [];
  asanaTasks.forEach((taskRow) => {
    boardRowsFromTasks.push(...asanaBoardRowsFromTaskRaw(taskRow));
  });
  let asanaBoards = mergeBoardRows(
    normalizeBoardRows(parseJsonNode(asanaBoardsNode, [])),
    boardRowsFromTasks
  );
  let asanaResourceOptions = normalizeResourceOptions(parseJsonNode(resourceOptionsNode, []));
  let boardResourceMappings = normalizeMappingMap(parseJsonNode(boardMappingsNode, {}));
  let taskResourceMappings = normalizeMappingMap(parseJsonNode(taskMappingsNode, {}));
  let plannerExternalItems = [];
  let plannerController = null;
  const workspaceMembersCache = new Map();

  const asanaResourceLookup = () => {
    const lookup = new Map();
    asanaResourceOptions.forEach((option) => {
      lookup.set(option.resource_uuid, option.resource_name);
    });
    return lookup;
  };

  const asanaTaskRowByGid = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!gid) return null;
    return asanaTasks.find((row) => row && String(row.gid || '').trim() === gid) || null;
  };

  const asanaBoardRowByGid = (boardGid) => {
    const gid = String(boardGid || '').trim();
    if (!gid) return null;
    return asanaBoards.find((row) => row && String(row.gid || '').trim() === gid) || null;
  };

  const asanaBoardRowsForTask = (taskRow) => {
    return asanaBoardRowsFromTaskRaw(taskRow).map((board) => {
      const catalogRow = asanaBoardRowByGid(board.gid);
      if (!catalogRow) return board;
      return {
        ...board,
        name: String(board.name || catalogRow.name || '').trim(),
        url: String(board.url || catalogRow.url || '').trim(),
      };
    });
  };

  const taskResourceUuids = (taskRow) => {
    if (!taskRow || typeof taskRow !== 'object') return [];
    const taskGid = String(taskRow.gid || '').trim();
    const boardRows = asanaBoardRowsForTask(taskRow);
    const combined = [];
    const seen = new Set();

    if (taskGid && Array.isArray(taskResourceMappings[taskGid])) {
      taskResourceMappings[taskGid].forEach((raw) => {
        const normalized = normalizeUuid(raw);
        if (!normalized || seen.has(normalized)) return;
        seen.add(normalized);
        combined.push(normalized);
      });
    }

    boardRows.forEach((board) => {
      const mapped = boardResourceMappings[board.gid];
      if (!Array.isArray(mapped)) return;
      mapped.forEach((raw) => {
        const normalized = normalizeUuid(raw);
        if (!normalized || seen.has(normalized)) return;
        seen.add(normalized);
        combined.push(normalized);
      });
    });

    return combined;
  };

  const taskResourceNames = (taskRow) => {
    const lookup = asanaResourceLookup();
    return taskResourceUuids(taskRow)
      .map((resourceUuidValue) => String(lookup.get(resourceUuidValue) || '').trim())
      .filter(Boolean);
  };

  const taskMappedToResource = (taskRow) => {
    if (!resourceUuid) return false;
    return taskResourceUuids(taskRow).includes(resourceUuid);
  };

  const boardMappedToResource = (boardGid) => {
    if (!resourceUuid) return false;
    const gid = String(boardGid || '').trim();
    if (!gid) return false;
    return Array.isArray(boardResourceMappings[gid]) && boardResourceMappings[gid].includes(resourceUuid);
  };

  const classifyTaskKind = (taskRow) => {
    const section = String(taskRow && taskRow.section_name ? taskRow.section_name : '').trim().toLowerCase();
    const boardNames = asanaBoardRowsForTask(taskRow)
      .map((row) => String(row.name || '').trim().toLowerCase())
      .join(' ');
    const combined = `${section} ${boardNames}`.trim();
    if (!combined) return 'follow-up';
    if (/(meeting|sync|standup|planning)/.test(combined)) return 'meeting';
    if (/(review|retro|qa|audit)/.test(combined)) return 'review';
    if (/(release|deploy|delivery|launch|ship)/.test(combined)) return 'delivery';
    return 'follow-up';
  };

  const asanaTaskGidForItem = (item) => String(
    item && (item.taskGid || item.task_gid || item.gid)
      ? (item.taskGid || item.task_gid || item.gid)
      : ''
  ).trim();

  const asanaCompleteUrlForTask = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!asanaCompleteUrlTemplate || !gid) return '';
    return asanaCompleteUrlTemplate.replace('__TASK_GID__', encodeURIComponent(gid));
  };

  const asanaCreateTaskUrlForBoard = (boardGid) => {
    const gid = String(boardGid || '').trim();
    if (!asanaCreateTaskUrlTemplate || !gid) return '';
    return asanaCreateTaskUrlTemplate.replace('__BOARD_GID__', encodeURIComponent(gid));
  };

  const asanaDeleteUrlForTask = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!asanaDeleteTaskUrlTemplate || !gid) return '';
    return asanaDeleteTaskUrlTemplate.replace('__TASK_GID__', encodeURIComponent(gid));
  };

  const asanaCommentsUrlForTask = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!asanaCommentsUrlTemplate || !gid) return '';
    return asanaCommentsUrlTemplate.replace('__TASK_GID__', encodeURIComponent(gid));
  };

  const asanaCommentAddUrlForTask = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!asanaCommentAddUrlTemplate || !gid) return '';
    return asanaCommentAddUrlTemplate.replace('__TASK_GID__', encodeURIComponent(gid));
  };

  const asanaBoardMapUrlForBoard = (boardGid) => {
    const gid = String(boardGid || '').trim();
    if (!asanaBoardMapUrlTemplate || !gid) return '';
    return asanaBoardMapUrlTemplate.replace('__BOARD_GID__', encodeURIComponent(gid));
  };

  const asanaTaskMapUrlForTask = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!asanaTaskMapUrlTemplate || !gid) return '';
    return asanaTaskMapUrlTemplate.replace('__TASK_GID__', encodeURIComponent(gid));
  };

  const asanaWorkspaceMembersUrlForWorkspace = (workspaceGid) => {
    const gid = String(workspaceGid || '').trim();
    if (!asanaWorkspaceMembersUrlTemplate || !gid) return '';
    return asanaWorkspaceMembersUrlTemplate.replace('__WORKSPACE_GID__', encodeURIComponent(gid));
  };

  const isTaskCompletedWithinWindow = (taskRow) => {
    if (!taskRow || typeof taskRow !== 'object' || !Boolean(taskRow.completed)) {
      return false;
    }
    const completedAtEpoch = toEpoch(taskRow.completed_at);
    if (!completedAtEpoch) return false;
    const cutoffEpoch = Date.now() - (completedWindowDays * 24 * 60 * 60 * 1000);
    return completedAtEpoch >= cutoffEpoch;
  };

  const postJson = async (url, payload) => {
    const response = await fetch(url, {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': getCookie('csrftoken'),
        'X-Requested-With': 'XMLHttpRequest',
      },
      body: JSON.stringify(payload || {}),
    });
    const parsed = await response.json().catch(() => ({}));
    if (!response.ok || !parsed || parsed.ok !== true) {
      throw new Error(String(parsed && parsed.error ? parsed.error : 'request_failed'));
    }
    return parsed;
  };

  const fetchJson = async (url) => {
    const response = await fetch(url, {
      method: 'GET',
      credentials: 'same-origin',
      headers: {
        'X-Requested-With': 'XMLHttpRequest',
      },
    });
    const parsed = await response.json().catch(() => ({}));
    if (!response.ok || !parsed || parsed.ok !== true) {
      throw new Error(String(parsed && parsed.error ? parsed.error : 'request_failed'));
    }
    return parsed;
  };

  const loadWorkspaceMembers = async (workspaceGid) => {
    const gid = String(workspaceGid || '').trim();
    if (!gid) return [];
    if (workspaceMembersCache.has(gid)) {
      return workspaceMembersCache.get(gid);
    }
    const membersUrl = asanaWorkspaceMembersUrlForWorkspace(gid);
    if (!membersUrl) {
      workspaceMembersCache.set(gid, []);
      return [];
    }
    const payload = await fetchJson(membersUrl);
    const seen = new Set();
    const members = (Array.isArray(payload && payload.members) ? payload.members : [])
      .map((member) => ({
        gid: String(member && member.gid ? member.gid : '').trim(),
        name: String(member && member.name ? member.name : '').trim(),
        email: String(member && member.email ? member.email : '').trim(),
      }))
      .filter((member) => {
        if (!member.gid || seen.has(member.gid)) return false;
        seen.add(member.gid);
        return true;
      })
      .sort((left, right) => {
        const leftName = String(left.name || left.email || left.gid).trim().toLowerCase();
        const rightName = String(right.name || right.email || right.gid).trim().toLowerCase();
        return leftName.localeCompare(rightName);
      });
    workspaceMembersCache.set(gid, members);
    return members;
  };

  const buildPlannerExternalItems = () => asanaTasks
    .filter((row) => row && typeof row === 'object')
    .filter((row) => taskMappedToResource(row))
    .map((row) => {
      const gid = String(row.gid || '').trim();
      const dueDate = String(row.due_date || '').trim();
      if (!gid || !dueDate) return null;
      return {
        id: `asana-task-${gid}`,
        title: String(row.name || '').trim() || `Asana task ${gid}`,
        date: dueDate,
        time: String(row.due_time || '').trim(),
        kind: classifyTaskKind(row),
        done: Boolean(row.completed),
        completed_at: String(row.completed_at || '').trim(),
        source: 'asana',
        taskGid: gid,
        url: String(row.task_url || '').trim(),
        resource_uuids: taskResourceUuids(row),
      };
    })
    .filter(Boolean);

  const refreshPlannerExternalItems = () => {
    plannerExternalItems = buildPlannerExternalItems();
    if (plannerController && typeof plannerController.setExternalItems === 'function') {
      plannerController.setExternalItems(plannerExternalItems);
    }
  };

  const upsertAsanaTaskRow = (taskRow) => {
    if (!taskRow || typeof taskRow !== 'object') return;
    const gid = String(taskRow.gid || '').trim();
    if (!gid) return;
    const normalizedRow = {
      ...taskRow,
      gid,
      name: String(taskRow.name || '').trim(),
      due_date: String(taskRow.due_date || '').trim(),
      due_time: String(taskRow.due_time || '').trim(),
      due_display: String(taskRow.due_display || '').trim(),
      task_url: String(taskRow.task_url || '').trim(),
      completed: Boolean(taskRow.completed),
      completed_at: String(taskRow.completed_at || '').trim(),
    };
    let replaced = false;
    asanaTasks = asanaTasks.map((row) => {
      if (!row || String(row.gid || '').trim() !== gid) return row;
      replaced = true;
      return {
        ...row,
        ...normalizedRow,
      };
    });
    if (!replaced) {
      asanaTasks = [...asanaTasks, normalizedRow];
    }
    asanaBoards = mergeBoardRows(asanaBoards, asanaBoardRowsFromTaskRaw(normalizedRow));
  };

  const removeAsanaTaskRow = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!gid) return;
    asanaTasks = asanaTasks.filter((row) => String(row && row.gid ? row.gid : '').trim() !== gid);
    if (Object.prototype.hasOwnProperty.call(taskResourceMappings, gid)) {
      const nextMappings = { ...taskResourceMappings };
      delete nextMappings[gid];
      taskResourceMappings = nextMappings;
    }
  };

  const syncAsanaTaskState = (taskGid, completed, completedAt) => {
    const gid = String(taskGid || '').trim();
    if (!gid) return;
    const nextDone = Boolean(completed);
    const nextCompletedAt = nextDone ? String(completedAt || new Date().toISOString()).trim() : '';

    const row = asanaTaskRowByGid(gid);
    if (!row) return;
    upsertAsanaTaskRow({
      ...row,
      completed: nextDone,
      completed_at: nextCompletedAt,
      status_label: nextDone ? 'Completed' : 'Open',
      status_tone: nextDone ? 'success' : 'info',
    });
    refreshPlannerExternalItems();
  };

  const syncBoardMapping = (boardGid, resourceUuids) => {
    const gid = String(boardGid || '').trim();
    if (!gid) return;
    boardResourceMappings = {
      ...boardResourceMappings,
      [gid]: normalizeUuidList(resourceUuids),
    };
  };

  const syncTaskMapping = (taskGid, resourceUuids) => {
    const gid = String(taskGid || '').trim();
    if (!gid) return;
    taskResourceMappings = {
      ...taskResourceMappings,
      [gid]: normalizeUuidList(resourceUuids),
    };
  };

  const asanaTaskUrlForTask = (taskGid) => {
    const row = asanaTaskRowByGid(taskGid);
    if (!row) return '';
    return String(row.task_url || '').trim();
  };

  const asanaBoardLinkByGid = (boardGid) => {
    const gid = String(boardGid || '').trim();
    if (!gid) return { url: '', name: '' };
    const boardRow = asanaBoardRowByGid(gid);
    if (boardRow) {
      return {
        url: String(boardRow.url || '').trim(),
        name: String(boardRow.name || '').trim(),
      };
    }
    for (let index = 0; index < asanaTasks.length; index += 1) {
      const row = asanaTasks[index];
      const boards = asanaBoardRowsForTask(row);
      for (let boardIndex = 0; boardIndex < boards.length; boardIndex += 1) {
        const board = boards[boardIndex];
        if (String(board && board.gid ? board.gid : '').trim() !== gid) continue;
        return {
          url: String(board && board.url ? board.url : '').trim(),
          name: String(board && board.name ? board.name : '').trim(),
        };
      }
    }
    return { url: '', name: '' };
  };

  const asanaBoardLinkForTask = (taskGid) => {
    const row = asanaTaskRowByGid(taskGid);
    if (!row) {
      return { url: '', name: '' };
    }
    const boards = asanaBoardRowsForTask(row);
    for (let i = 0; i < boards.length; i += 1) {
      const board = boards[i];
      if (!board || !board.url) continue;
      return { url: String(board.url || '').trim(), name: String(board.name || '').trim() };
    }
    return { url: '', name: '' };
  };

  const closeRuntimeModal = () => {
    const activeModal = document.querySelector('[data-runtime-modal]');
    if (!activeModal) return;
    activeModal.remove();
    document.body.classList.remove('modal-open');
  };

  const openRuntimeModal = (title) => {
    closeRuntimeModal();
    const modal = document.createElement('div');
    modal.className = 'modal is-active';
    modal.setAttribute('data-runtime-modal', '1');
    modal.setAttribute('aria-hidden', 'false');

    const overlay = document.createElement('button');
    overlay.type = 'button';
    overlay.className = 'modal-overlay';
    overlay.setAttribute('aria-label', 'Close dialog');
    overlay.addEventListener('click', closeRuntimeModal);

    const content = document.createElement('div');
    content.className = 'modal-content modal-content-wide';
    content.setAttribute('role', 'dialog');
    content.setAttribute('aria-modal', 'true');

    const header = document.createElement('div');
    header.className = 'modal-header';
    const heading = document.createElement('h3');
    heading.className = 'detail-card-title';
    heading.textContent = title;
    const closeBtn = document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'icon-btn';
    closeBtn.textContent = '✕';
    closeBtn.setAttribute('aria-label', 'Close dialog');
    closeBtn.addEventListener('click', closeRuntimeModal);
    header.appendChild(heading);
    header.appendChild(closeBtn);

    const body = document.createElement('div');
    body.className = 'modal-form';

    const actions = document.createElement('div');
    actions.className = 'modal-actions';

    content.appendChild(header);
    content.appendChild(body);
    content.appendChild(actions);
    modal.appendChild(overlay);
    modal.appendChild(content);
    document.body.appendChild(modal);
    document.body.classList.add('modal-open');

    return { modal, body, actions, close: closeRuntimeModal };
  };

  const openAsanaCreateTaskModal = async (section) => {
    const boardGid = String(section && (section.boardGid || section.board_gid) ? (section.boardGid || section.board_gid) : '').trim();
    const rawBoardName = String(section && (section.boardName || section.board_name || section.title) ? (section.boardName || section.board_name || section.title) : '').trim();
    const boardName = rawBoardName.replace(/^Asana\s*-\s*/i, '').trim() || 'Asana board';
    if (!boardGid) {
      throw new Error('missing_board_gid');
    }
    const createUrl = asanaCreateTaskUrlForBoard(boardGid);
    if (!createUrl) {
      throw new Error('missing_create_endpoint');
    }

    const modalApi = openRuntimeModal(`Create Asana Task · ${boardName}`);
    const form = document.createElement('form');
    form.className = 'modal-form';
    form.innerHTML = `
      <label>
        <span>Task name</span>
        <input type="text" maxlength="500" required placeholder="New Asana task" />
      </label>
      <label>
        <span>Due date (optional)</span>
        <input type="date" />
      </label>
      <label>
        <span>Due time (optional)</span>
        <input type="time" />
      </label>
      <label>
        <span>Notes (optional)</span>
        <textarea rows="3" maxlength="5000" placeholder="Task details"></textarea>
      </label>
    `;
    modalApi.body.appendChild(form);

    const nameInput = form.querySelector('input[type="text"]');
    const dueDateInput = form.querySelector('input[type="date"]');
    const dueTimeInput = form.querySelector('input[type="time"]');
    const notesInput = form.querySelector('textarea');

    const createBtn = document.createElement('button');
    createBtn.type = 'button';
    createBtn.className = 'primary-btn';
    createBtn.textContent = 'Create task';
    createBtn.addEventListener('click', () => {
      if (typeof form.requestSubmit === 'function') {
        form.requestSubmit();
      } else {
        form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
      }
    });
    modalApi.actions.appendChild(createBtn);

    const cancelBtn = document.createElement('button');
    cancelBtn.type = 'button';
    cancelBtn.className = 'ghost-btn';
    cancelBtn.textContent = 'Cancel';
    cancelBtn.addEventListener('click', modalApi.close);
    modalApi.actions.appendChild(cancelBtn);

    const boardLink = asanaBoardLinkByGid(boardGid);
    const boardRow = asanaBoardRowByGid(boardGid);
    const boardWorkspaceGid = String(
      (boardRow && (boardRow.workspace_gid || boardRow.workspaceGid))
      || (section && (section.workspace_gid || section.workspaceGid))
      || ''
    ).trim();
    let assigneeSelect = null;
    if (isSuperuser) {
      const assigneeField = document.createElement('label');
      const assigneeLabel = document.createElement('span');
      assigneeLabel.textContent = 'Assignee (superuser only)';
      assigneeField.appendChild(assigneeLabel);

      assigneeSelect = document.createElement('select');
      const meOption = document.createElement('option');
      meOption.value = '';
      meOption.textContent = 'Me (default)';
      assigneeSelect.appendChild(meOption);
      assigneeField.appendChild(assigneeSelect);
      form.appendChild(assigneeField);

      if (boardWorkspaceGid) {
        try {
          const members = await loadWorkspaceMembers(boardWorkspaceGid);
          members.forEach((member) => {
            const memberGid = String(member.gid || '').trim();
            if (!memberGid) return;
            const option = document.createElement('option');
            option.value = memberGid;
            option.textContent = String(member.name || member.email || memberGid).trim();
            assigneeSelect.appendChild(option);
          });
        } catch (error) {
          assigneeSelect.disabled = true;
          const warning = document.createElement('div');
          warning.className = 'text-muted small';
          warning.textContent = 'Unable to load workspace members. Task will be assigned to you.';
          assigneeField.appendChild(warning);
        }
      } else {
        assigneeSelect.disabled = true;
        const warning = document.createElement('div');
        warning.className = 'text-muted small';
        warning.textContent = 'Board workspace unavailable. Task will be assigned to you.';
        assigneeField.appendChild(warning);
      }
    }
    if (boardLink.url) {
      const openBoardBtn = document.createElement('a');
      openBoardBtn.className = 'ghost-btn';
      openBoardBtn.href = boardLink.url;
      openBoardBtn.target = '_blank';
      openBoardBtn.rel = 'noopener noreferrer';
      openBoardBtn.textContent = 'Open board';
      modalApi.actions.appendChild(openBoardBtn);
    }

    form.addEventListener('submit', async (event) => {
      event.preventDefault();
      const taskName = String(nameInput && nameInput.value ? nameInput.value : '').trim();
      if (!taskName) return;
      const dueDate = String(dueDateInput && dueDateInput.value ? dueDateInput.value : '').trim();
      const dueTime = String(dueTimeInput && dueTimeInput.value ? dueTimeInput.value : '').trim();
      const notes = String(notesInput && notesInput.value ? notesInput.value : '').trim();
      const assigneeGid = String(
        isSuperuser && assigneeSelect && !assigneeSelect.disabled && assigneeSelect.value
          ? assigneeSelect.value
          : ''
      ).trim();
      const wasAssigneeDisabled = Boolean(assigneeSelect && assigneeSelect.disabled);

      createBtn.disabled = true;
      if (assigneeSelect) assigneeSelect.disabled = true;
      try {
        const createRequest = {
          name: taskName,
          due_date: dueDate,
          due_time: dueTime,
          notes,
        };
        if (assigneeGid) {
          createRequest.assignee_gid = assigneeGid;
        }
        const payload = await postJson(createUrl, createRequest);
        const selectedAssigneeName = assigneeGid && assigneeSelect
          ? String(
            assigneeSelect.options[assigneeSelect.selectedIndex]
              ? assigneeSelect.options[assigneeSelect.selectedIndex].textContent
              : ''
          ).trim()
          : '';
        const taskRow = payload && payload.task && typeof payload.task === 'object'
          ? payload.task
          : {
            gid: String(payload && payload.task_gid ? payload.task_gid : '').trim(),
            name: taskName,
            due_date: dueDate,
            due_time: dueTime,
            completed: false,
            completed_at: '',
            project_links: [{ gid: boardGid, name: boardName, url: boardLink.url || '' }],
            assignee_gid: assigneeGid,
            assignee_name: selectedAssigneeName,
          };
        upsertAsanaTaskRow(taskRow);
        refreshPlannerExternalItems();
        modalApi.close();
      } catch (error) {
        window.alert(`Unable to create Asana task: ${String(error && error.message ? error.message : 'request_failed')}`);
      } finally {
        if (assigneeSelect) assigneeSelect.disabled = wasAssigneeDisabled;
        createBtn.disabled = false;
      }
    });

    if (nameInput) {
      nameInput.focus();
    }
  };

  const openAsanaDeleteTaskModal = async (item) => {
    const taskGid = asanaTaskGidForItem(item);
    if (!taskGid) {
      throw new Error('missing_task_gid');
    }
    const deleteUrl = asanaDeleteUrlForTask(taskGid);
    if (!deleteUrl) {
      throw new Error('missing_delete_endpoint');
    }

    const taskRow = asanaTaskRowByGid(taskGid);
    const taskName = String(taskRow && taskRow.name ? taskRow.name : item && item.title ? item.title : `Task ${taskGid}`).trim();
    const modalApi = openRuntimeModal(`Delete Asana Task · ${taskName}`);
    const warning = document.createElement('p');
    warning.className = 'text-muted';
    warning.textContent = 'This will permanently delete the task in Asana. This action cannot be undone.';
    modalApi.body.appendChild(warning);

    const deleteBtn = document.createElement('button');
    deleteBtn.type = 'button';
    deleteBtn.className = 'primary-btn';
    deleteBtn.textContent = 'Delete task';
    modalApi.actions.appendChild(deleteBtn);

    const cancelBtn = document.createElement('button');
    cancelBtn.type = 'button';
    cancelBtn.className = 'ghost-btn';
    cancelBtn.textContent = 'Cancel';
    cancelBtn.addEventListener('click', modalApi.close);
    modalApi.actions.appendChild(cancelBtn);

    const externalTaskUrl = asanaTaskUrlForTask(taskGid);
    if (externalTaskUrl) {
      const openTaskBtn = document.createElement('a');
      openTaskBtn.className = 'ghost-btn';
      openTaskBtn.href = externalTaskUrl;
      openTaskBtn.target = '_blank';
      openTaskBtn.rel = 'noopener noreferrer';
      openTaskBtn.textContent = 'Open in Asana';
      modalApi.actions.appendChild(openTaskBtn);
    }

    deleteBtn.addEventListener('click', async () => {
      deleteBtn.disabled = true;
      try {
        await postJson(deleteUrl, {});
        removeAsanaTaskRow(taskGid);
        refreshPlannerExternalItems();
        modalApi.close();
      } catch (error) {
        window.alert(`Unable to delete Asana task: ${String(error && error.message ? error.message : 'request_failed')}`);
      } finally {
        deleteBtn.disabled = false;
      }
    });
  };

  const openAsanaCommentsModal = async (item) => {
    const taskGid = asanaTaskGidForItem(item);
    const taskRow = asanaTaskRowByGid(taskGid);
    const taskName = String(taskRow && taskRow.name ? taskRow.name : item && item.title ? item.title : '').trim() || `Task ${taskGid}`;
    if (!taskGid) return;
    const commentsUrl = asanaCommentsUrlForTask(taskGid);
    const replyUrl = asanaCommentAddUrlForTask(taskGid);
    if (!commentsUrl || !replyUrl) {
      throw new Error('comments_endpoint_missing');
    }

    const modalApi = openRuntimeModal(`Asana Comments · ${taskName}`);
    const commentsList = document.createElement('div');
    commentsList.className = 'planner-comments-list';
    commentsList.innerHTML = '<p class="text-muted">Loading comments…</p>';

    const form = document.createElement('form');
    form.className = 'modal-form';
    const replyLabel = document.createElement('label');
    const replyTitle = document.createElement('span');
    replyTitle.textContent = 'Reply';
    const textarea = document.createElement('textarea');
    textarea.rows = 4;
    textarea.maxLength = 5000;
    textarea.placeholder = 'Add a comment to this Asana task…';
    replyLabel.appendChild(replyTitle);
    replyLabel.appendChild(textarea);
    form.appendChild(replyLabel);

    const submitBtn = document.createElement('button');
    submitBtn.type = 'submit';
    submitBtn.className = 'primary-btn';
    submitBtn.textContent = 'Post reply';
    modalApi.actions.appendChild(submitBtn);

    const closeBtn = document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'ghost-btn';
    closeBtn.textContent = 'Close';
    closeBtn.addEventListener('click', modalApi.close);
    modalApi.actions.appendChild(closeBtn);

    const externalTaskUrl = asanaTaskUrlForTask(taskGid);
    if (externalTaskUrl) {
      const openTaskBtn = document.createElement('a');
      openTaskBtn.className = 'ghost-btn';
      openTaskBtn.href = externalTaskUrl;
      openTaskBtn.target = '_blank';
      openTaskBtn.rel = 'noopener noreferrer';
      openTaskBtn.textContent = 'Open in Asana';
      modalApi.actions.appendChild(openTaskBtn);
    }

    modalApi.body.appendChild(commentsList);
    modalApi.body.appendChild(form);

    const renderComments = (comments) => {
      commentsList.innerHTML = '';
      if (!Array.isArray(comments) || comments.length === 0) {
        commentsList.innerHTML = '<p class="text-muted">No comments yet.</p>';
        return;
      }
      comments.forEach((comment) => {
        const row = document.createElement('article');
        row.className = 'planner-comment-item';

        const head = document.createElement('div');
        head.className = 'planner-comment-item__head';
        const author = document.createElement('strong');
        author.textContent = String(comment && comment.author_name ? comment.author_name : 'Asana user');
        const when = document.createElement('span');
        when.className = 'text-muted small';
        when.textContent = String(comment && comment.created_display ? comment.created_display : formatWhen(comment && comment.created_at ? comment.created_at : ''));
        head.appendChild(author);
        head.appendChild(when);

        const text = document.createElement('p');
        text.className = 'planner-comment-item__text';
        text.textContent = String(comment && comment.text ? comment.text : '').trim();

        row.appendChild(head);
        row.appendChild(text);
        commentsList.appendChild(row);
      });
    };

    try {
      const payload = await fetchJson(commentsUrl);
      renderComments(payload.comments || []);
    } catch (error) {
      commentsList.innerHTML = `<p class="text-muted">Unable to load comments (${String(error && error.message ? error.message : 'request_failed')}).</p>`;
    }

    form.addEventListener('submit', async (event) => {
      event.preventDefault();
      const text = String(textarea.value || '').trim();
      if (!text) return;
      submitBtn.disabled = true;
      try {
        const payload = await postJson(replyUrl, { text });
        textarea.value = '';
        const currentComments = Array.from(commentsList.querySelectorAll('.planner-comment-item'));
        if (!currentComments.length) {
          renderComments([payload.comment]);
        } else {
          const row = document.createElement('article');
          row.className = 'planner-comment-item';
          const head = document.createElement('div');
          head.className = 'planner-comment-item__head';
          const author = document.createElement('strong');
          author.textContent = String(payload && payload.comment && payload.comment.author_name ? payload.comment.author_name : 'Asana user');
          const when = document.createElement('span');
          when.className = 'text-muted small';
          when.textContent = String(payload && payload.comment && payload.comment.created_display ? payload.comment.created_display : formatWhen(new Date().toISOString()));
          head.appendChild(author);
          head.appendChild(when);
          const body = document.createElement('p');
          body.className = 'planner-comment-item__text';
          body.textContent = String(payload && payload.comment && payload.comment.text ? payload.comment.text : text);
          row.appendChild(head);
          row.appendChild(body);
          commentsList.appendChild(row);
        }
      } catch (error) {
        window.alert(`Unable to post Asana comment: ${String(error && error.message ? error.message : 'request_failed')}`);
      } finally {
        submitBtn.disabled = false;
      }
    });
  };

  const openAsanaResourceMappingModal = async (item) => {
    const taskGid = asanaTaskGidForItem(item);
    const taskRow = asanaTaskRowByGid(taskGid);
    if (!taskGid || !taskRow) return;

    const boards = asanaBoardRowsForTask(taskRow);
    const modalApi = openRuntimeModal(`Resource Mapping · ${String(taskRow.name || item.title || `Task ${taskGid}`)}`);

    if (!asanaResourceOptions.length) {
      const empty = document.createElement('p');
      empty.className = 'text-muted';
      empty.textContent = 'No accessible resources found. Add resources first, then map this task.';
      modalApi.body.appendChild(empty);
    }

    const renderCheckboxGroup = (title, selectedUuids) => {
      const wrap = document.createElement('fieldset');
      wrap.className = 'modal-fieldset';
      const legend = document.createElement('legend');
      legend.textContent = title;
      wrap.appendChild(legend);
      asanaResourceOptions.forEach((option) => {
        const row = document.createElement('label');
        row.className = 'checkbox';
        const input = document.createElement('input');
        input.type = 'checkbox';
        input.value = option.resource_uuid;
        if (selectedUuids.has(option.resource_uuid)) {
          input.checked = true;
        }
        const text = document.createElement('span');
        text.textContent = option.resource_name;
        row.appendChild(input);
        row.appendChild(text);
        wrap.appendChild(row);
      });
      return wrap;
    };

    const boardFieldsets = [];
    boards.forEach((board) => {
      const selected = new Set(normalizeUuidList(boardResourceMappings[board.gid]));
      const fieldset = renderCheckboxGroup(`Board: ${board.name}`, selected);
      boardFieldsets.push({ board, fieldset });
      modalApi.body.appendChild(fieldset);
    });

    const selectedTaskUuids = new Set(normalizeUuidList(taskResourceMappings[taskGid]));
    const taskFieldset = renderCheckboxGroup('Additional resources for this task', selectedTaskUuids);
    modalApi.body.appendChild(taskFieldset);

    const saveBtn = document.createElement('button');
    saveBtn.type = 'button';
    saveBtn.className = 'primary-btn';
    saveBtn.textContent = 'Save mappings';
    modalApi.actions.appendChild(saveBtn);

    const closeBtn = document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'ghost-btn';
    closeBtn.textContent = 'Close';
    closeBtn.addEventListener('click', modalApi.close);
    modalApi.actions.appendChild(closeBtn);

    const selectedFromFieldset = (fieldset) => Array.from(fieldset.querySelectorAll('input[type="checkbox"]:checked'))
      .map((input) => normalizeUuid(input.value))
      .filter(Boolean);

    saveBtn.addEventListener('click', async () => {
      saveBtn.disabled = true;
      try {
        for (let idx = 0; idx < boardFieldsets.length; idx += 1) {
          const entry = boardFieldsets[idx];
          const boardGid = String(entry.board && entry.board.gid ? entry.board.gid : '').trim();
          const url = asanaBoardMapUrlForBoard(boardGid);
          if (!boardGid || !url) continue;
          const selected = selectedFromFieldset(entry.fieldset);
          const payload = await postJson(url, { resource_uuids: selected });
          syncBoardMapping(boardGid, Array.isArray(payload.resource_uuids) ? payload.resource_uuids : selected);
        }

        const taskUrl = asanaTaskMapUrlForTask(taskGid);
        if (taskUrl) {
          const selectedTask = selectedFromFieldset(taskFieldset);
          const payload = await postJson(taskUrl, { resource_uuids: selectedTask });
          syncTaskMapping(taskGid, Array.isArray(payload.resource_uuids) ? payload.resource_uuids : selectedTask);
        }

        refreshPlannerExternalItems();
        modalApi.close();
      } catch (error) {
        window.alert(`Unable to save mapping: ${String(error && error.message ? error.message : 'request_failed')}`);
      } finally {
        saveBtn.disabled = false;
      }
    });
  };

  const handleAsanaToggleError = (item, error) => {
    const taskName = String(item && item.title ? item.title : 'task');
    const errorMessage = String(error && error.message ? error.message : 'update_failed');
    console.warn(`Unable to update ${taskName}: ${errorMessage}`);
    const gid = asanaTaskGidForItem(item);
    const boardLink = asanaBoardLinkForTask(gid);
    const boardUrl = String(boardLink.url || '').trim();
    const boardName = String(boardLink.name || '').trim() || 'Asana board';
    const fallbackTaskUrl = String(item && item.url ? item.url : '').trim();
    const destinationUrl = boardUrl || fallbackTaskUrl;
    const connectorMsg = `There was an issue with the Asana connector while updating "${taskName}".`;
    if (destinationUrl && typeof window.confirm === 'function') {
      const prompt = `${connectorMsg}\n\nPlease update this task from ${boardName}.\n\nOpen ${boardName} now?`;
      if (window.confirm(prompt)) {
        window.open(destinationUrl, '_blank', 'noopener,noreferrer');
      }
      return;
    }
    if (typeof window.alert === 'function') {
      const suffix = destinationUrl ? `\n\nUpdate in Asana: ${destinationUrl}` : '';
      window.alert(`${connectorMsg}\n\nPlease update this task directly in Asana.${suffix}`);
    }
  };

  const openAsanaAgendaSections = (context) => {
    const rawView = String(context && context.view ? context.view : '').trim().toLowerCase();
    const view = (rawView === 'all' || rawView === 'month-list') ? 'all' : 'tasks';
    const activeFilter = String(context && context.activeFilter ? context.activeFilter : 'all').trim().toLowerCase();
    if (activeFilter !== 'all') return [];
    const includeCompletedWindow = true;
    const todayKey = toYmdLocal(new Date());
    const selectedDateKey = String(context && context.selectedDate ? context.selectedDate : '').trim() || todayKey;
    const tasksStartKey = selectedDateKey;
    const tasksEndKey = addDaysToYmd(tasksStartKey, 13) || tasksStartKey;
    const allHistoryStartKey = addDaysToYmd(todayKey, -(completedWindowDays - 1)) || todayKey;

    const candidateRows = asanaTasks
      .filter((row) => row && typeof row === 'object')
      .filter((row) => taskMappedToResource(row))
      .filter((row) => {
        const completed = Boolean(row.completed);
        const dueDate = String(row && row.due_date ? row.due_date : '').trim();
        if (view === 'all') {
          if (!completed) return true;
          return includeCompletedWindow && isTaskCompletedWithinWindow(row);
        }
        if (!dueDate) return true;
        return dueDate >= tasksStartKey && dueDate <= tasksEndKey;
      });

    const normalizedBoardId = (value) => String(value || '').trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
    const boardBuckets = new Map();
    const ensureBoardBucket = (board) => {
      const boardName = String(board && board.name ? board.name : 'Unassigned').trim() || 'Unassigned';
      const boardGid = String(board && board.gid ? board.gid : '').trim();
      const boardKey = boardGid || normalizedBoardId(boardName) || 'unassigned';
      const sectionId = `asana-board-${boardKey}`;
      if (!boardBuckets.has(sectionId)) {
        boardBuckets.set(sectionId, {
          id: sectionId,
          title: `Asana - ${boardName}`,
          boardGid,
          boardName,
          actions: boardGid ? [{ id: 'create-task', label: '+', title: `Add task to ${boardName}`, iconOnly: true }] : [],
          items: [],
        });
      }
      return boardBuckets.get(sectionId);
    };
    asanaBoards
      .filter((board) => boardMappedToResource(board && board.gid ? board.gid : ''))
      .forEach((board) => {
        ensureBoardBucket(board);
      });

    candidateRows.forEach((row) => {
      const gid = String(row.gid || '').trim();
      if (!gid) return;
      const dueDisplay = String(row.due_display || '').trim();
      const sectionName = String(row.section_name || '').trim();
      const workspaceName = String(row.workspace_name || '').trim();
      const dueDate = String(row.due_date || '').trim();
      const dueTime = String(row.due_time || '').trim();
      const completedAt = String(row.completed_at || '').trim();
      const completed = Boolean(row.completed);

      const metaParts = [];
      if (completed) {
        metaParts.push(completedAt ? `Completed ${formatWhen(completedAt)}` : 'Completed');
      } else {
        metaParts.push(dueDisplay ? `Due ${dueDisplay}` : 'No due date');
      }
      if (sectionName) {
        metaParts.push(sectionName);
      } else if (workspaceName) {
        metaParts.push(workspaceName);
      }

      const mappedResources = taskResourceNames(row);
      if (mappedResources.length) {
        metaParts.push(`Resources: ${mappedResources.join(', ')}`);
      }

      const sortDate = completed ? (completedAt || '9999-12-31T23:59:59Z') : (dueDate || '9999-12-31');
      const sortTime = completed ? '99:99' : (dueTime || '99:99');
      const sortPrefix = completed ? '1' : '0';
      const sortDirection = completed ? `${9999999999999 - toEpoch(sortDate)}` : `${sortDate} ${sortTime}`;
      const item = {
        id: `asana-agenda-${gid}`,
        title: String(row.name || '').trim() || `Asana task ${gid}`,
        meta: metaParts.join(' · '),
        source: 'asana',
        taskGid: gid,
        url: String(row.task_url || '').trim(),
        done: completed,
        canToggle: true,
        isExternal: true,
        actions: [
          { id: 'comments', label: 'Comments' },
          { id: 'resources', label: 'Resources' },
          { id: 'delete', label: 'Delete' },
        ],
        sortKey: `${sortPrefix}-${sortDirection}-${String(row.name || '').trim().toLowerCase()}`,
      };

      const boards = asanaBoardRowsForTask(row);
      if (!boards.length) {
        ensureBoardBucket({ name: 'Unassigned', gid: '' }).items.push(item);
        return;
      }
      boards.forEach((board) => {
        ensureBoardBucket(board).items.push(item);
      });
    });

    return Array.from(boardBuckets.values())
      .sort((left, right) => String(left.title || '').localeCompare(String(right.title || '')))
      .map((section) => ({
        ...section,
        emptyText: section.boardGid ? 'No tasks in this board.' : 'No tasks in this section.',
        items: section.items
          .slice()
          .sort((left, right) => String(left.sortKey || '').localeCompare(String(right.sortKey || '')))
          .map(({ sortKey, ...rest }) => rest),
      }));
  };

  if (!window.AlshivalPlanner || typeof window.AlshivalPlanner.init !== 'function') {
    return;
  }

  refreshPlannerExternalItems();
  plannerController = window.AlshivalPlanner.init(plannerRoot, {
    storageKey: plannerStorageKey,
    listHistoryDays: 30,
    monthEmptyText: 'No resource items for this day.',
    listEmptyText: 'No resource tasks in the last 30 days or upcoming.',
    seedItems: () => [],
    externalItems: () => plannerExternalItems,
    agendaSections: openAsanaAgendaSections,
    onExternalToggle: async (item, done) => {
      const gid = asanaTaskGidForItem(item);
      if (!gid) return;
      const url = asanaCompleteUrlForTask(gid);
      if (!url) throw new Error('missing_asana_task');
      const payload = await postJson(url, { completed: Boolean(done) });
      syncAsanaTaskState(gid, done, String(payload && payload.completed_at ? payload.completed_at : ''));
    },
    onAgendaSectionAction: async (item, actionId, section) => {
      const normalizedAction = String(actionId || '').trim().toLowerCase();
      if (normalizedAction === 'create-task') {
        await openAsanaCreateTaskModal(section || {});
        return;
      }
      if (normalizedAction === 'delete') {
        await openAsanaDeleteTaskModal(item);
        return;
      }
      if (normalizedAction === 'comments') {
        await openAsanaCommentsModal(item);
        return;
      }
      if (normalizedAction === 'resources') {
        await openAsanaResourceMappingModal(item);
      }
    },
    onToggleError: handleAsanaToggleError,
    onActionError: (item, actionId, error) => {
      const taskName = String(item && item.title ? item.title : 'selected board');
      const actionName = String(actionId || 'action').trim();
      const message = String(error && error.message ? error.message : 'request_failed');
      window.alert(`Unable to run ${actionName} for "${taskName}": ${message}`);
    },
    showListTimelineItems: (context) => String(context && context.activeFilter ? context.activeFilter : 'all')
      .trim()
      .toLowerCase() !== 'all',
  });
})();
