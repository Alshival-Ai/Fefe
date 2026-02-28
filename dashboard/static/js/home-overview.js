(() => {
  const dashboard = document.querySelector('[data-overview-dashboard]');
  if (!dashboard) {
    return;
  }

  const plannerRoot = dashboard.querySelector('[data-overview-planner]');

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

  const plannerExternalDataNode = document.getElementById('overview-planner-external-items');
  const asanaTasksDataNode = document.getElementById('overview-asana-tasks');
  const asanaBoardsDataNode = document.getElementById('overview-asana-boards');
  const asanaResourceOptionsNode = document.getElementById('overview-asana-resource-options');
  const agendaItemMappingsNode = document.getElementById('overview-agenda-item-resource-mappings');
  const asanaBoardMappingsNode = document.getElementById('overview-asana-board-resource-mappings');
  const asanaTaskMappingsNode = document.getElementById('overview-asana-task-resource-mappings');

  const asanaCompleteUrlTemplate = dashboard.getAttribute('data-asana-complete-url-template') || '';
  const asanaCreateTaskUrlTemplate = dashboard.getAttribute('data-asana-board-task-create-url-template') || '';
  const asanaDeleteTaskUrlTemplate = dashboard.getAttribute('data-asana-task-delete-url-template') || '';
  const asanaCommentsUrlTemplate = dashboard.getAttribute('data-asana-comments-url-template') || '';
  const asanaCommentAddUrlTemplate = dashboard.getAttribute('data-asana-comment-add-url-template') || '';
  const asanaBoardMapUrlTemplate = dashboard.getAttribute('data-asana-board-map-url-template') || '';
  const asanaTaskMapUrlTemplate = dashboard.getAttribute('data-asana-task-map-url-template') || '';
  const asanaBoardSectionsUrlTemplate = dashboard.getAttribute('data-asana-board-sections-url-template') || '';
  const asanaSectionAddTaskUrlTemplate = dashboard.getAttribute('data-asana-section-add-task-url-template') || '';
  const asanaSubtasksUrlTemplate = dashboard.getAttribute('data-asana-subtasks-url-template') || '';
  const asanaAttachmentsUrlTemplate = dashboard.getAttribute('data-asana-attachments-url-template') || '';
  const asanaDependenciesUrlTemplate = dashboard.getAttribute('data-asana-dependencies-url-template') || '';
  const asanaDependencyAddUrlTemplate = dashboard.getAttribute('data-asana-dependency-add-url-template') || '';
  const asanaDependencyRemoveUrlTemplate = dashboard.getAttribute('data-asana-dependency-remove-url-template') || '';
  const asanaAssignUrlTemplate = dashboard.getAttribute('data-asana-assign-url-template') || '';
  const asanaWorkspaceMembersUrlTemplate = dashboard.getAttribute('data-asana-workspace-members-url-template') || '';
  const agendaItemMapUrl = dashboard.getAttribute('data-agenda-item-map-url') || '';
  const completedWindowDays = Math.max(
    1,
    Math.min(60, Number.parseInt(String(dashboard.getAttribute('data-asana-completed-window-days') || '14'), 10) || 14)
  );
  const plannerStorageKey = 'overview_planner_items';

  let plannerExternalItems = [];
  let asanaTasks = [];
  let asanaBoards = [];
  let asanaResourceOptions = [];
  let agendaItemResourceMappings = {};
  let boardResourceMappings = {};
  let taskResourceMappings = {};
  let plannerController = null;
  const outlookTeamsJoinByAgendaItemId = new Map();

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
          workspace_name: String(rawBoard.workspace_name || rawBoard.workspaceName || '').trim(),
        };
        const existing = mergedByGid.get(gid);
        if (!existing) {
          mergedByGid.set(gid, normalized);
          return;
        }
        if (!existing.url && normalized.url) existing.url = normalized.url;
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

  plannerExternalItems = parseJsonNode(plannerExternalDataNode, []);
  asanaTasks = parseJsonNode(asanaTasksDataNode, []);
  const boardRowsFromTasks = [];
  asanaTasks.forEach((taskRow) => {
    boardRowsFromTasks.push(...asanaBoardRowsFromTaskRaw(taskRow));
  });
  asanaBoards = mergeBoardRows(
    normalizeBoardRows(parseJsonNode(asanaBoardsDataNode, [])),
    boardRowsFromTasks
  );
  asanaResourceOptions = normalizeResourceOptions(parseJsonNode(asanaResourceOptionsNode, []));
  agendaItemResourceMappings = normalizeMappingMap(parseJsonNode(agendaItemMappingsNode, {}));
  boardResourceMappings = normalizeMappingMap(parseJsonNode(asanaBoardMappingsNode, {}));
  taskResourceMappings = normalizeMappingMap(parseJsonNode(asanaTaskMappingsNode, {}));

  const asanaResourceLookup = () => {
    const lookup = new Map();
    asanaResourceOptions.forEach((option) => {
      lookup.set(option.resource_uuid, option.resource_name);
    });
    return lookup;
  };

  const mergeResourceUuidLists = (...lists) => {
    const merged = [];
    const seen = new Set();
    lists.forEach((value) => {
      normalizeUuidList(value).forEach((resourceUuid) => {
        if (!resourceUuid || seen.has(resourceUuid)) return;
        seen.add(resourceUuid);
        merged.push(resourceUuid);
      });
    });
    return merged;
  };

  const resourceNamesForUuids = (resourceUuids) => {
    const lookup = asanaResourceLookup();
    return normalizeUuidList(resourceUuids)
      .map((resourceUuid) => String(lookup.get(resourceUuid) || '').trim())
      .filter(Boolean);
  };

  const toEpoch = (value) => {
    const resolved = String(value || '').trim();
    if (!resolved) return 0;
    const parsed = Date.parse(resolved);
    return Number.isFinite(parsed) ? parsed : 0;
  };

  const toYmdLocal = (value) => {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  };

  const formatTimelineWhen = (value) => {
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

  const toBool = (value) => {
    if (typeof value === 'boolean') return value;
    const normalized = String(value || '').trim().toLowerCase();
    return normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'on';
  };

  const plannerItemSource = (item) => String(item && item.source ? item.source : '').trim().toLowerCase();

  const agendaItemIdForItem = (item) => String(item && item.id ? item.id : '').trim();

  const agendaItemResourceUuids = (itemOrItemId) => {
    const itemId = typeof itemOrItemId === 'string'
      ? String(itemOrItemId || '').trim()
      : agendaItemIdForItem(itemOrItemId);
    if (!itemId) return [];
    return normalizeUuidList(agendaItemResourceMappings[itemId]);
  };

  const syncAgendaItemMapping = (itemId, resourceUuids) => {
    const resolvedItemId = String(itemId || '').trim();
    if (!resolvedItemId) return;
    agendaItemResourceMappings = {
      ...agendaItemResourceMappings,
      [resolvedItemId]: normalizeUuidList(resourceUuids),
    };
  };

  const outlookJoinUrlForItem = (item) => {
    if (!item || typeof item !== 'object') return '';
    return String(
      item.teams_join_url
      || item.teamsJoinUrl
      || item.online_meeting_url
      || item.onlineMeetingUrl
      || ''
    ).trim();
  };

  const outlookEventUrlForItem = (item) => {
    if (!item || typeof item !== 'object') return '';
    return String(item.event_url || item.eventUrl || item.url || '').trim();
  };

  const stripLegacyPlannerSeeds = () => {
    try {
      const raw = window.localStorage.getItem(plannerStorageKey) || '';
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return;
      const filtered = parsed.filter((item) => {
        const itemId = String(item && item.id ? item.id : '').trim();
        return !itemId.startsWith('overview-seed-');
      });
      if (filtered.length !== parsed.length) {
        window.localStorage.setItem(plannerStorageKey, JSON.stringify(filtered));
      }
    } catch (error) {
      // Ignore local storage parse/write failures.
    }
  };

  const asanaTaskGidForItem = (item) => {
    const direct = String(
      item && (item.taskGid || item.task_gid || item.gid) ? (item.taskGid || item.task_gid || item.gid) : ''
    ).trim();
    if (direct) return direct;
    return '';
  };

  const asanaTaskUrlForTask = (taskGid) => {
    const row = asanaTaskRowByGid(taskGid);
    if (!row) return '';
    return String(row.task_url || '').trim();
  };

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

  const asanaBoardSectionsUrlForBoard = (boardGid) => {
    const gid = String(boardGid || '').trim();
    if (!asanaBoardSectionsUrlTemplate || !gid) return '';
    return asanaBoardSectionsUrlTemplate.replace('__BOARD_GID__', encodeURIComponent(gid));
  };

  const asanaSectionAddTaskUrlForSection = (sectionGid) => {
    const gid = String(sectionGid || '').trim();
    if (!asanaSectionAddTaskUrlTemplate || !gid) return '';
    return asanaSectionAddTaskUrlTemplate.replace('__SECTION_GID__', encodeURIComponent(gid));
  };

  const asanaSubtasksUrlForTask = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!asanaSubtasksUrlTemplate || !gid) return '';
    return asanaSubtasksUrlTemplate.replace('__TASK_GID__', encodeURIComponent(gid));
  };

  const asanaAttachmentsUrlForTask = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!asanaAttachmentsUrlTemplate || !gid) return '';
    return asanaAttachmentsUrlTemplate.replace('__TASK_GID__', encodeURIComponent(gid));
  };

  const asanaDependenciesUrlForTask = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!asanaDependenciesUrlTemplate || !gid) return '';
    return asanaDependenciesUrlTemplate.replace('__TASK_GID__', encodeURIComponent(gid));
  };

  const asanaDependencyAddUrlForTask = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!asanaDependencyAddUrlTemplate || !gid) return '';
    return asanaDependencyAddUrlTemplate.replace('__TASK_GID__', encodeURIComponent(gid));
  };

  const asanaDependencyRemoveUrlForTask = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!asanaDependencyRemoveUrlTemplate || !gid) return '';
    return asanaDependencyRemoveUrlTemplate.replace('__TASK_GID__', encodeURIComponent(gid));
  };

  const asanaAssignUrlForTask = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!asanaAssignUrlTemplate || !gid) return '';
    return asanaAssignUrlTemplate.replace('__TASK_GID__', encodeURIComponent(gid));
  };

  const asanaWorkspaceMembersUrlForWorkspace = (workspaceGid) => {
    const gid = String(workspaceGid || '').trim();
    if (!asanaWorkspaceMembersUrlTemplate || !gid) return '';
    return asanaWorkspaceMembersUrlTemplate.replace('__WORKSPACE_GID__', encodeURIComponent(gid));
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

  const combinedTaskResourceUuids = (taskRow, agendaItemId = '') => (
    mergeResourceUuidLists(taskResourceUuids(taskRow), agendaItemResourceUuids(agendaItemId))
  );

  const taskResourceNames = (taskRow, agendaItemId = '') => {
    return resourceNamesForUuids(combinedTaskResourceUuids(taskRow, agendaItemId));
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

  const plannerExternalItemFromTaskRow = (taskRow) => {
    if (!taskRow || typeof taskRow !== 'object') return null;
    const gid = String(taskRow.gid || '').trim();
    const dueDate = String(taskRow.due_date || '').trim();
    if (!gid || !dueDate) return null;
    return {
      id: `asana-task-${gid}`,
      title: String(taskRow.name || '').trim() || `Asana task ${gid}`,
      date: dueDate,
      time: String(taskRow.due_time || '').trim(),
      kind: classifyTaskKind(taskRow),
      done: Boolean(taskRow.completed),
      completed_at: String(taskRow.completed_at || '').trim(),
      source: 'asana',
      taskGid: gid,
      url: String(taskRow.task_url || '').trim(),
    };
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

  const upsertPlannerExternalItem = (taskRow) => {
    const externalItem = plannerExternalItemFromTaskRow(taskRow);
    const gid = String(taskRow && taskRow.gid ? taskRow.gid : '').trim();
    if (!gid) return;
    if (!externalItem) {
      plannerExternalItems = plannerExternalItems.filter((item) => asanaTaskGidForItem(item) !== gid);
      return;
    }
    let replaced = false;
    plannerExternalItems = plannerExternalItems.map((item) => {
      if (asanaTaskGidForItem(item) !== gid) return item;
      replaced = true;
      return {
        ...item,
        ...externalItem,
      };
    });
    if (!replaced) {
      plannerExternalItems = [...plannerExternalItems, externalItem];
    }
  };

  const removePlannerExternalItem = (taskGid) => {
    const gid = String(taskGid || '').trim();
    if (!gid) return;
    plannerExternalItems = plannerExternalItems.filter((item) => asanaTaskGidForItem(item) !== gid);
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
    const updatedRow = asanaTaskRowByGid(gid);
    if (updatedRow) {
      upsertPlannerExternalItem(updatedRow);
    }
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

  const updateAsanaTaskCompletion = async (item, completed) => {
    const gid = asanaTaskGidForItem(item);
    const url = asanaCompleteUrlForTask(gid);
    if (!gid || !url) {
      throw new Error('missing_asana_task');
    }
    return postJson(url, { completed: Boolean(completed) });
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
    if (dueDateInput) {
      dueDateInput.value = '';
    }

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
    if (boardLink.url) {
      const boardBtn = document.createElement('a');
      boardBtn.className = 'ghost-btn';
      boardBtn.href = boardLink.url;
      boardBtn.target = '_blank';
      boardBtn.rel = 'noopener noreferrer';
      boardBtn.textContent = 'Open board';
      modalApi.actions.appendChild(boardBtn);
    }

    form.addEventListener('submit', async (event) => {
      event.preventDefault();
      const taskName = String(nameInput && nameInput.value ? nameInput.value : '').trim();
      if (!taskName) return;
      const dueDate = String(dueDateInput && dueDateInput.value ? dueDateInput.value : '').trim();
      const dueTime = String(dueTimeInput && dueTimeInput.value ? dueTimeInput.value : '').trim();
      const notes = String(notesInput && notesInput.value ? notesInput.value : '').trim();

      createBtn.disabled = true;
      try {
        const payload = await postJson(createUrl, {
          name: taskName,
          due_date: dueDate,
          due_time: dueTime,
          notes,
        });
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
          };
        upsertAsanaTaskRow(taskRow);
        const updatedRow = asanaTaskRowByGid(String(taskRow && taskRow.gid ? taskRow.gid : '').trim());
        if (updatedRow) {
          upsertPlannerExternalItem(updatedRow);
        }
        if (plannerController && typeof plannerController.setExternalItems === 'function') {
          plannerController.setExternalItems(plannerExternalItems);
        }
        modalApi.close();
      } catch (error) {
        window.alert(`Unable to create Asana task: ${String(error && error.message ? error.message : 'request_failed')}`);
      } finally {
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
        removePlannerExternalItem(taskGid);
        if (plannerController && typeof plannerController.setExternalItems === 'function') {
          plannerController.setExternalItems(plannerExternalItems);
        }
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

    const quickReacts = document.createElement('div');
    quickReacts.className = 'planner-comment-quickreact';
    const quickReactLabel = document.createElement('span');
    quickReactLabel.className = 'planner-comment-quickreact__label';
    quickReactLabel.textContent = 'Quick react:';
    quickReacts.appendChild(quickReactLabel);
    [
      { label: '🎉', text: '🎉' },
      { label: '👍', text: '👍' },
      { label: '✅', text: '✅' },
      { label: 'Awesome!', text: 'Awesome!' },
      { label: 'Great work!', text: 'Great work!' },
    ].forEach(({ label, text }) => {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'planner-comment-quickreact__btn';
      btn.textContent = label;
      btn.addEventListener('click', () => {
        textarea.value = text;
        if (typeof form.requestSubmit === 'function') {
          form.requestSubmit();
        } else {
          form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
        }
      });
      quickReacts.appendChild(btn);
    });
    form.appendChild(quickReacts);

    const replyLabel = document.createElement('label');
    const replyTitle = document.createElement('span');
    replyTitle.textContent = 'Reply';
    const textarea = document.createElement('textarea');
    textarea.rows = 3;
    textarea.maxLength = 5000;
    textarea.placeholder = 'Add a comment to this Asana task…';
    replyLabel.appendChild(replyTitle);
    replyLabel.appendChild(textarea);
    form.appendChild(replyLabel);

    const submitBtn = document.createElement('button');
    submitBtn.type = 'button';
    submitBtn.className = 'primary-btn';
    submitBtn.textContent = 'Post reply';
    submitBtn.addEventListener('click', (event) => {
      event.preventDefault();
      if (typeof form.requestSubmit === 'function') {
        form.requestSubmit();
      } else {
        form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
      }
    });
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
        when.textContent = String(comment && comment.created_display ? comment.created_display : formatTimelineWhen(comment && comment.created_at ? comment.created_at : ''));
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
          when.textContent = String(payload && payload.comment && payload.comment.created_display ? payload.comment.created_display : formatTimelineWhen(new Date().toISOString()));
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

  let activeTaskDrawer = null;

  const closeTaskDrawer = () => {
    if (activeTaskDrawer && activeTaskDrawer.parentNode) {
      activeTaskDrawer.parentNode.removeChild(activeTaskDrawer);
    }
    activeTaskDrawer = null;
    document.removeEventListener('keydown', handleDrawerEscape);
  };

  const handleDrawerEscape = (event) => {
    if (event.key === 'Escape') closeTaskDrawer();
  };

  const openAsanaTaskDrawer = (item) => {
    closeTaskDrawer();

    const taskGid = asanaTaskGidForItem(item);
    if (!taskGid) return;
    const taskRow = asanaTaskRowByGid(taskGid) || {};
    const taskName = String(taskRow.name || item.title || `Task ${taskGid}`).trim();
    const taskUrl = String(taskRow.task_url || item.url || '').trim();
    const workspaceGid = String(taskRow.workspace_gid || '').trim();

    // ── Overlay ──────────────────────────────────────────────────
    const overlay = document.createElement('div');
    overlay.className = 'task-drawer';
    overlay.addEventListener('click', (event) => {
      if (event.target === overlay) closeTaskDrawer();
    });

    // ── Panel ─────────────────────────────────────────────────────
    const panel = document.createElement('div');
    panel.className = 'task-drawer__panel';
    panel.setAttribute('role', 'dialog');
    panel.setAttribute('aria-modal', 'true');
    panel.setAttribute('aria-label', taskName);
    overlay.appendChild(panel);

    // ── Header ────────────────────────────────────────────────────
    const header = document.createElement('div');
    header.className = 'task-drawer__header';

    const titleSpan = document.createElement('span');
    titleSpan.className = 'task-drawer__title';
    titleSpan.textContent = taskName;
    header.appendChild(titleSpan);

    if (taskUrl) {
      const extLink = document.createElement('a');
      extLink.className = 'task-drawer__ext-link';
      extLink.href = taskUrl;
      extLink.target = '_blank';
      extLink.rel = 'noopener noreferrer';
      extLink.title = 'Open in Asana';
      extLink.textContent = '↗';
      header.appendChild(extLink);
    }

    const closeBtn = document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'task-drawer__close';
    closeBtn.setAttribute('aria-label', 'Close task detail');
    closeBtn.textContent = '✕';
    closeBtn.addEventListener('click', closeTaskDrawer);
    header.appendChild(closeBtn);
    panel.appendChild(header);

    // ── Body ──────────────────────────────────────────────────────
    const body = document.createElement('div');
    body.className = 'task-drawer__body';
    panel.appendChild(body);

    // ── Meta grid ─────────────────────────────────────────────────
    const metaGrid = document.createElement('div');
    metaGrid.className = 'task-drawer__meta-grid';

    const dueLabel = document.createElement('span');
    dueLabel.textContent = 'Due:';
    const dueVal = document.createElement('span');
    dueVal.textContent = String(taskRow.due_display || taskRow.due_date || '—').trim() || '—';
    metaGrid.appendChild(dueLabel);
    metaGrid.appendChild(dueVal);

    const assigneeLabel = document.createElement('span');
    assigneeLabel.textContent = 'Assignee:';
    const assigneeCell = document.createElement('span');

    const assigneeBtn = document.createElement('button');
    assigneeBtn.type = 'button';
    assigneeBtn.className = 'task-drawer__assignee-btn';
    assigneeBtn.textContent = String(taskRow.assignee_name || 'Unassigned').trim();
    let membersCache = null;
    assigneeBtn.addEventListener('click', async () => {
      assigneeBtn.disabled = true;
      try {
        if (!membersCache) {
          const membersUrl = asanaWorkspaceMembersUrlForWorkspace(workspaceGid);
          if (membersUrl) {
            const payload = await fetchJson(membersUrl);
            membersCache = Array.isArray(payload.members) ? payload.members : [];
          } else {
            membersCache = [];
          }
        }
        const select = document.createElement('select');
        select.className = 'task-drawer__assignee-select';
        const unassignedOpt = document.createElement('option');
        unassignedOpt.value = '';
        unassignedOpt.textContent = '— Unassigned —';
        select.appendChild(unassignedOpt);
        membersCache.forEach((member) => {
          const opt = document.createElement('option');
          opt.value = String(member.gid || '').trim();
          opt.textContent = String(member.name || member.email || member.gid).trim();
          if (opt.value === String(taskRow.assignee_gid || '').trim()) opt.selected = true;
          select.appendChild(opt);
        });
        select.addEventListener('change', async () => {
          const selectedGid = select.value;
          const assignUrl = asanaAssignUrlForTask(taskGid);
          if (!assignUrl) return;
          select.disabled = true;
          try {
            await postJson(assignUrl, { assignee_gid: selectedGid || null });
            const selectedMember = membersCache.find((m) => String(m.gid || '').trim() === selectedGid);
            const newName = selectedMember ? String(selectedMember.name || selectedMember.email || '').trim() : '';
            taskRow.assignee_gid = selectedGid;
            taskRow.assignee_name = newName;
            assigneeBtn.textContent = newName || 'Unassigned';
            assigneeCell.replaceChild(assigneeBtn, select);
            if (plannerController && typeof plannerController.refresh === 'function') {
              plannerController.refresh();
            }
          } catch (error) {
            window.alert(`Unable to update assignee: ${String(error && error.message ? error.message : 'request_failed')}`);
            select.disabled = false;
          }
        });
        assigneeCell.replaceChild(select, assigneeBtn);
        select.focus();
      } catch (error) {
        window.alert(`Unable to load workspace members: ${String(error && error.message ? error.message : 'request_failed')}`);
        assigneeBtn.disabled = false;
      }
    });
    assigneeCell.appendChild(assigneeBtn);
    metaGrid.appendChild(assigneeLabel);
    metaGrid.appendChild(assigneeCell);

    const boardLabel = document.createElement('span');
    boardLabel.textContent = 'Board:';
    const boardVal = document.createElement('span');
    const boardLink = asanaBoardLinkForTask(taskGid);
    if (boardLink && boardLink.url) {
      const boardA = document.createElement('a');
      boardA.href = boardLink.url;
      boardA.target = '_blank';
      boardA.rel = 'noopener noreferrer';
      boardA.textContent = boardLink.name || boardLink.url;
      boardVal.appendChild(boardA);
    } else {
      boardVal.textContent = boardLink && boardLink.name ? boardLink.name : '—';
    }
    metaGrid.appendChild(boardLabel);
    metaGrid.appendChild(boardVal);

    const sectionLabel = document.createElement('span');
    sectionLabel.textContent = 'Section:';
    const sectionVal = document.createElement('span');
    sectionVal.textContent = String(taskRow.section_name || '—').trim() || '—';
    metaGrid.appendChild(sectionLabel);
    metaGrid.appendChild(sectionVal);

    body.appendChild(metaGrid);

    // ── Notes ─────────────────────────────────────────────────────
    const notesTitle = document.createElement('div');
    notesTitle.className = 'task-drawer__section-title';
    notesTitle.textContent = 'Notes';
    body.appendChild(notesTitle);

    const notesEl = document.createElement('div');
    notesEl.className = 'task-drawer__notes';
    const notesText = String(taskRow.notes || '').trim();
    notesEl.textContent = notesText || 'No description.';
    if (!notesText) notesEl.classList.add('text-muted');
    body.appendChild(notesEl);

    // ── Helper: skeleton placeholder ──────────────────────────────
    const makeSkeleton = (text) => {
      const el = document.createElement('p');
      el.className = 'text-muted task-drawer__skeleton';
      el.textContent = text || 'Loading…';
      return el;
    };

    // ── Subtasks ──────────────────────────────────────────────────
    const subtaskCount = Number(taskRow.subtask_count || 0);
    const subtasksTitle = document.createElement('div');
    subtasksTitle.className = 'task-drawer__section-title';
    subtasksTitle.textContent = `Subtasks (${subtaskCount})`;
    body.appendChild(subtasksTitle);

    const subtaskList = document.createElement('div');
    subtaskList.className = 'task-drawer__subtask-list';
    subtaskList.appendChild(makeSkeleton('Loading subtasks…'));
    body.appendChild(subtaskList);

    // ── Attachments ───────────────────────────────────────────────
    const attachTitle = document.createElement('div');
    attachTitle.className = 'task-drawer__section-title';
    attachTitle.textContent = 'Attachments';
    body.appendChild(attachTitle);

    const attachList = document.createElement('div');
    attachList.className = 'task-drawer__attachment-list';
    attachList.appendChild(makeSkeleton('Loading attachments…'));
    body.appendChild(attachList);

    // ── Dependencies ──────────────────────────────────────────────
    const depsTitle = document.createElement('div');
    depsTitle.className = 'task-drawer__section-title';
    depsTitle.textContent = 'Dependencies';
    body.appendChild(depsTitle);

    const depList = document.createElement('div');
    depList.className = 'task-drawer__dep-list';
    depList.appendChild(makeSkeleton('Loading dependencies…'));
    body.appendChild(depList);

    const depAddRow = document.createElement('div');
    depAddRow.className = 'task-drawer__dep-add';
    const depInput = document.createElement('input');
    depInput.type = 'text';
    depInput.className = 'task-drawer__dep-input';
    depInput.placeholder = 'Dependency task GID…';
    depInput.maxLength = 64;
    const depAddBtn = document.createElement('button');
    depAddBtn.type = 'button';
    depAddBtn.className = 'primary-btn task-drawer__dep-add-btn';
    depAddBtn.textContent = 'Add';
    depAddBtn.addEventListener('click', async () => {
      const depGid = String(depInput.value || '').trim();
      if (!depGid) return;
      const addUrl = asanaDependencyAddUrlForTask(taskGid);
      if (!addUrl) return;
      depAddBtn.disabled = true;
      try {
        await postJson(addUrl, { dependency_gid: depGid });
        depInput.value = '';
        const depsUrl = asanaDependenciesUrlForTask(taskGid);
        if (depsUrl) {
          const payload = await fetchJson(depsUrl);
          renderDeps(Array.isArray(payload.dependencies) ? payload.dependencies : []);
        }
      } catch (error) {
        window.alert(`Unable to add dependency: ${String(error && error.message ? error.message : 'request_failed')}`);
      } finally {
        depAddBtn.disabled = false;
      }
    });
    depAddRow.appendChild(depInput);
    depAddRow.appendChild(depAddBtn);
    body.appendChild(depAddRow);

    // ── Comments ──────────────────────────────────────────────────
    const commentsTitle = document.createElement('div');
    commentsTitle.className = 'task-drawer__section-title';
    commentsTitle.textContent = 'Comments';
    body.appendChild(commentsTitle);

    const commentList = document.createElement('div');
    commentList.className = 'task-drawer__comment-list';
    commentList.appendChild(makeSkeleton('Loading comments…'));
    body.appendChild(commentList);

    // Quick reacts + comment form
    const commentForm = document.createElement('div');
    commentForm.className = 'task-drawer__comment-form';

    const quickReacts = document.createElement('div');
    quickReacts.className = 'planner-comment-quickreact';
    const quickReactLabel = document.createElement('span');
    quickReactLabel.className = 'planner-comment-quickreact__label';
    quickReactLabel.textContent = 'Quick react:';
    quickReacts.appendChild(quickReactLabel);

    const commentTextarea = document.createElement('textarea');
    commentTextarea.className = 'task-drawer__comment-textarea';
    commentTextarea.rows = 3;
    commentTextarea.maxLength = 5000;
    commentTextarea.placeholder = 'Add a comment…';

    [
      { label: '🎉', text: '🎉' },
      { label: '👍', text: '👍' },
      { label: '✅', text: '✅' },
      { label: 'Awesome!', text: 'Awesome!' },
      { label: 'Great work!', text: 'Great work!' },
    ].forEach(({ label, text }) => {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'planner-comment-quickreact__btn';
      btn.textContent = label;
      btn.addEventListener('click', () => { commentTextarea.value = text; });
      quickReacts.appendChild(btn);
    });

    const commentPostBtn = document.createElement('button');
    commentPostBtn.type = 'button';
    commentPostBtn.className = 'primary-btn task-drawer__comment-post';
    commentPostBtn.textContent = 'Post';

    commentForm.appendChild(quickReacts);
    commentForm.appendChild(commentTextarea);
    commentForm.appendChild(commentPostBtn);
    body.appendChild(commentForm);

    // ── Render helpers ────────────────────────────────────────────
    const renderSubtasks = (subtasks) => {
      subtaskList.innerHTML = '';
      subtasksTitle.textContent = `Subtasks (${subtasks.length})`;
      if (!subtasks.length) {
        subtaskList.appendChild(makeSkeleton('No subtasks.'));
        return;
      }
      subtasks.forEach((sub) => {
        const subGid = String(sub.gid || '').trim();
        const subDone = Boolean(sub.completed);
        const row = document.createElement('div');
        row.className = `task-drawer__subtask-row${subDone ? ' is-done' : ''}`;

        const chk = document.createElement('input');
        chk.type = 'checkbox';
        chk.checked = subDone;
        chk.setAttribute('aria-label', `Mark ${String(sub.name || subGid)} as complete`);
        chk.addEventListener('change', async () => {
          const completeUrl = asanaCompleteUrlForTask(subGid);
          if (!completeUrl) { chk.checked = subDone; return; }
          chk.disabled = true;
          try {
            await postJson(completeUrl, { completed: chk.checked });
            if (chk.checked) row.classList.add('is-done'); else row.classList.remove('is-done');
          } catch (error) {
            chk.checked = !chk.checked;
            window.alert(`Unable to update subtask: ${String(error && error.message ? error.message : 'request_failed')}`);
          } finally {
            chk.disabled = false;
          }
        });

        const subTitle = document.createElement('span');
        subTitle.className = 'task-drawer__subtask-title';
        subTitle.textContent = String(sub.name || subGid).trim();

        row.appendChild(chk);
        row.appendChild(subTitle);
        subtaskList.appendChild(row);
      });
    };

    const renderAttachments = (attachments) => {
      attachList.innerHTML = '';
      if (!attachments.length) {
        attachList.appendChild(makeSkeleton('No attachments.'));
        return;
      }
      attachments.forEach((att) => {
        const row = document.createElement('div');
        row.className = 'task-drawer__attachment-row';
        const link = document.createElement('a');
        link.href = String(att.download_url || att.view_url || att.url || '#');
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        link.textContent = String(att.name || att.filename || 'Attachment');
        row.appendChild(link);
        attachList.appendChild(row);
      });
    };

    const renderDeps = (deps) => {
      depList.innerHTML = '';
      if (!deps.length) {
        depList.appendChild(makeSkeleton('No dependencies.'));
        return;
      }
      deps.forEach((dep) => {
        const depGid = String(dep.gid || '').trim();
        const row = document.createElement('div');
        row.className = 'task-drawer__dep-row';
        const nameSpan = document.createElement('span');
        nameSpan.textContent = String(dep.name || depGid).trim();
        const removeBtn = document.createElement('button');
        removeBtn.type = 'button';
        removeBtn.className = 'ghost-btn task-drawer__dep-remove';
        removeBtn.textContent = '✕';
        removeBtn.setAttribute('aria-label', `Remove dependency ${String(dep.name || depGid)}`);
        removeBtn.addEventListener('click', async () => {
          const removeUrl = asanaDependencyRemoveUrlForTask(taskGid);
          if (!removeUrl) return;
          removeBtn.disabled = true;
          try {
            await postJson(removeUrl, { dependency_gid: depGid });
            row.parentNode && row.parentNode.removeChild(row);
          } catch (error) {
            window.alert(`Unable to remove dependency: ${String(error && error.message ? error.message : 'request_failed')}`);
            removeBtn.disabled = false;
          }
        });
        row.appendChild(nameSpan);
        row.appendChild(removeBtn);
        depList.appendChild(row);
      });
    };

    const renderComments = (comments) => {
      commentList.innerHTML = '';
      if (!comments.length) {
        commentList.appendChild(makeSkeleton('No comments yet.'));
        return;
      }
      comments.forEach((comment) => {
        const row = document.createElement('article');
        row.className = 'planner-comment-item';
        const head = document.createElement('div');
        head.className = 'planner-comment-item__head';
        const author = document.createElement('strong');
        author.textContent = String(comment.author_name || 'Asana user');
        const when = document.createElement('span');
        when.className = 'text-muted small';
        when.textContent = String(comment.created_display || formatTimelineWhen(comment.created_at || ''));
        head.appendChild(author);
        head.appendChild(when);
        const text = document.createElement('p');
        text.className = 'planner-comment-item__text';
        text.textContent = String(comment.text || '').trim();
        row.appendChild(head);
        row.appendChild(text);
        commentList.appendChild(row);
      });
    };

    commentPostBtn.addEventListener('click', async () => {
      const replyUrl = asanaCommentAddUrlForTask(taskGid);
      if (!replyUrl) return;
      const text = String(commentTextarea.value || '').trim();
      if (!text) return;
      commentPostBtn.disabled = true;
      try {
        const payload = await postJson(replyUrl, { text });
        commentTextarea.value = '';
        const newComment = (payload && payload.comment) ? payload.comment : { author_name: 'Asana user', text, created_at: new Date().toISOString() };
        const existingItems = commentList.querySelectorAll('.planner-comment-item');
        if (!existingItems.length) {
          renderComments([newComment]);
        } else {
          const row = document.createElement('article');
          row.className = 'planner-comment-item';
          const head = document.createElement('div');
          head.className = 'planner-comment-item__head';
          const author = document.createElement('strong');
          author.textContent = String(newComment.author_name || 'Asana user');
          const when = document.createElement('span');
          when.className = 'text-muted small';
          when.textContent = String(newComment.created_display || formatTimelineWhen(newComment.created_at || new Date().toISOString()));
          head.appendChild(author);
          head.appendChild(when);
          const textEl = document.createElement('p');
          textEl.className = 'planner-comment-item__text';
          textEl.textContent = String(newComment.text || text);
          row.appendChild(head);
          row.appendChild(textEl);
          commentList.appendChild(row);
        }
      } catch (error) {
        window.alert(`Unable to post comment: ${String(error && error.message ? error.message : 'request_failed')}`);
      } finally {
        commentPostBtn.disabled = false;
      }
    });

    // ── Attach to DOM ─────────────────────────────────────────────
    document.body.appendChild(overlay);
    activeTaskDrawer = overlay;
    document.addEventListener('keydown', handleDrawerEscape);

    // ── Async data fetches ────────────────────────────────────────
    const subtasksUrl = asanaSubtasksUrlForTask(taskGid);
    const attachmentsUrl = asanaAttachmentsUrlForTask(taskGid);
    const dependenciesUrl = asanaDependenciesUrlForTask(taskGid);
    const commentsUrl = asanaCommentsUrlForTask(taskGid);

    Promise.allSettled([
      subtasksUrl ? fetchJson(subtasksUrl) : Promise.resolve(null),
      attachmentsUrl ? fetchJson(attachmentsUrl) : Promise.resolve(null),
      dependenciesUrl ? fetchJson(dependenciesUrl) : Promise.resolve(null),
      commentsUrl ? fetchJson(commentsUrl) : Promise.resolve(null),
    ]).then(([subtasksResult, attachmentsResult, depsResult, commentsResult]) => {
      if (subtasksResult.status === 'fulfilled' && subtasksResult.value) {
        renderSubtasks(Array.isArray(subtasksResult.value.subtasks) ? subtasksResult.value.subtasks : []);
      } else {
        subtaskList.innerHTML = '';
        subtaskList.appendChild(makeSkeleton('Unable to load subtasks.'));
      }
      if (attachmentsResult.status === 'fulfilled' && attachmentsResult.value) {
        renderAttachments(Array.isArray(attachmentsResult.value.attachments) ? attachmentsResult.value.attachments : []);
      } else {
        attachList.innerHTML = '';
        attachList.appendChild(makeSkeleton('Unable to load attachments.'));
      }
      if (depsResult.status === 'fulfilled' && depsResult.value) {
        renderDeps(Array.isArray(depsResult.value.dependencies) ? depsResult.value.dependencies : []);
      } else {
        depList.innerHTML = '';
        depList.appendChild(makeSkeleton('Unable to load dependencies.'));
      }
      if (commentsResult.status === 'fulfilled' && commentsResult.value) {
        renderComments(Array.isArray(commentsResult.value.comments) ? commentsResult.value.comments : []);
      } else {
        commentList.innerHTML = '';
        commentList.appendChild(makeSkeleton('Unable to load comments.'));
      }
    });
  };

  const sourceItemIdForAgendaItem = (item) => {
    if (!item || typeof item !== 'object') return '';
    const direct = String(
      item.sourceItemId
      || item.source_item_id
      || item.taskGid
      || item.task_gid
      || item.eventId
      || item.event_id
      || ''
    ).trim();
    if (direct) return direct;
    const itemId = agendaItemIdForItem(item);
    if (itemId.startsWith('asana-agenda-')) {
      return itemId.slice('asana-agenda-'.length);
    }
    if (itemId.startsWith('outlook-agenda-')) {
      return itemId.slice('outlook-agenda-'.length);
    }
    return '';
  };

  const saveAgendaItemResourceMapping = async (item, resourceUuids) => {
    if (!agendaItemMapUrl) return normalizeUuidList(resourceUuids);
    const itemId = agendaItemIdForItem(item);
    if (!itemId) return normalizeUuidList(resourceUuids);
    const payload = await postJson(agendaItemMapUrl, {
      item: {
        item_id: itemId,
        source: plannerItemSource(item),
        source_item_id: sourceItemIdForAgendaItem(item),
        title: String(item && item.title ? item.title : '').trim(),
        date: String(item && item.date ? item.date : '').trim(),
        time: String(item && item.time ? item.time : '').trim(),
        due_at: String(item && item.dueAt ? item.dueAt : '').trim(),
        url: String(item && item.url ? item.url : '').trim(),
        meta: String(item && item.meta ? item.meta : '').trim(),
        done: Boolean(item && item.done),
      },
      resource_uuids: normalizeUuidList(resourceUuids),
    });
    return normalizeUuidList(
      Array.isArray(payload && payload.resource_uuids)
        ? payload.resource_uuids
        : resourceUuids
    );
  };

  const openAgendaResourceMappingModal = async (item) => {
    const itemId = agendaItemIdForItem(item);
    if (!itemId) return;

    const source = plannerItemSource(item);
    const taskGid = source === 'asana' ? asanaTaskGidForItem(item) : '';
    const taskRow = taskGid ? asanaTaskRowByGid(taskGid) : null;
    const selectedUuids = source === 'asana' && taskRow
      ? combinedTaskResourceUuids(taskRow, itemId)
      : agendaItemResourceUuids(itemId);
    const selectedSet = new Set(normalizeUuidList(selectedUuids));
    const modalTitle = `Attach Resources · ${String(item && item.title ? item.title : 'Agenda item').trim()}`;
    const modalApi = openRuntimeModal(modalTitle);

    if (!asanaResourceOptions.length) {
      const empty = document.createElement('p');
      empty.className = 'text-muted';
      empty.textContent = 'No accessible resources found. Add resources first, then attach them here.';
      modalApi.body.appendChild(empty);
    } else {
      const fieldset = document.createElement('fieldset');
      fieldset.className = 'modal-fieldset';
      const legend = document.createElement('legend');
      legend.textContent = 'Available resources';
      fieldset.appendChild(legend);
      const helper = document.createElement('p');
      helper.className = 'text-muted small';
      helper.textContent = 'Includes personal, team, and global resources you can access.';
      fieldset.appendChild(helper);
      asanaResourceOptions.forEach((option) => {
        const row = document.createElement('label');
        row.className = 'checkbox';
        const input = document.createElement('input');
        input.type = 'checkbox';
        input.value = option.resource_uuid;
        input.checked = selectedSet.has(option.resource_uuid);
        const text = document.createElement('span');
        text.textContent = option.resource_name;
        row.appendChild(input);
        row.appendChild(text);
        fieldset.appendChild(row);
      });
      modalApi.body.appendChild(fieldset);
    }

    const saveBtn = document.createElement('button');
    saveBtn.type = 'button';
    saveBtn.className = 'primary-btn';
    saveBtn.textContent = 'Save attachments';
    modalApi.actions.appendChild(saveBtn);

    const closeBtn = document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'ghost-btn';
    closeBtn.textContent = 'Close';
    closeBtn.addEventListener('click', modalApi.close);
    modalApi.actions.appendChild(closeBtn);

    if (item && item.url) {
      const openBtn = document.createElement('a');
      openBtn.className = 'ghost-btn';
      openBtn.href = item.url;
      openBtn.target = '_blank';
      openBtn.rel = 'noopener noreferrer';
      openBtn.textContent = source === 'asana' ? 'Open in Asana' : 'Open event';
      modalApi.actions.appendChild(openBtn);
    }

    const selectedFromModal = () => Array.from(modalApi.body.querySelectorAll('input[type="checkbox"]:checked'))
      .map((input) => normalizeUuid(input.value))
      .filter(Boolean);

    saveBtn.addEventListener('click', async () => {
      saveBtn.disabled = true;
      try {
        let selected = selectedFromModal();
        if (source === 'asana' && taskGid) {
          const taskUrl = asanaTaskMapUrlForTask(taskGid);
          if (taskUrl) {
            try {
              const payload = await postJson(taskUrl, { resource_uuids: selected });
              const savedTaskMap = normalizeUuidList(
                Array.isArray(payload && payload.resource_uuids) ? payload.resource_uuids : selected
              );
              syncTaskMapping(taskGid, savedTaskMap);
              selected = savedTaskMap;
            } catch (error) {
              // Keep going with agenda-level mapping even if Asana update fails.
            }
          }
        }
        const savedAgenda = await saveAgendaItemResourceMapping(item, selected);
        syncAgendaItemMapping(itemId, savedAgenda);
        if (plannerController && typeof plannerController.setExternalItems === 'function') {
          plannerController.setExternalItems(plannerExternalItems);
        }
        modalApi.close();
      } catch (error) {
        window.alert(`Unable to save resource attachments: ${String(error && error.message ? error.message : 'request_failed')}`);
      } finally {
        saveBtn.disabled = false;
      }
    });
  };

  const openAsanaMoveToSectionModal = async (item, section) => {
    const taskGid = asanaTaskGidForItem(item);
    if (!taskGid) throw new Error('missing_task_gid');
    const boardGid = String(section && (section.boardGid || section.board_gid) ? (section.boardGid || section.board_gid) : '').trim();
    if (!boardGid) throw new Error('missing_board_gid');

    const sectionsUrl = asanaBoardSectionsUrlForBoard(boardGid);
    if (!sectionsUrl) throw new Error('missing_sections_endpoint');

    let sectionsList = [];
    try {
      const sectionsPayload = await fetchJson(sectionsUrl);
      sectionsList = Array.isArray(sectionsPayload && sectionsPayload.sections) ? sectionsPayload.sections : [];
    } catch (error) {
      throw new Error(`Unable to load sections: ${String(error && error.message ? error.message : 'request_failed')}`);
    }

    const taskTitle = String(item && item.title ? item.title : 'task').trim();
    const rawBoardName = String(section && (section.boardName || section.title) ? (section.boardName || section.title) : '').trim();
    const boardName = rawBoardName.replace(/^Asana\s*-\s*/i, '').trim() || 'board';
    const modalApi = openRuntimeModal(`Move to Section · ${taskTitle}`);

    if (!sectionsList.length) {
      const empty = document.createElement('p');
      empty.className = 'text-muted';
      empty.textContent = `No sections found in "${boardName}".`;
      modalApi.body.appendChild(empty);
    } else {
      const label = document.createElement('label');
      const labelText = document.createElement('span');
      labelText.textContent = 'Choose a section';
      const select = document.createElement('select');
      select.className = 'modal-select';
      sectionsList.forEach((sec) => {
        const option = document.createElement('option');
        option.value = String(sec && sec.gid ? sec.gid : '').trim();
        option.textContent = String(sec && sec.name ? sec.name : '').trim() || option.value;
        select.appendChild(option);
      });
      label.appendChild(labelText);
      label.appendChild(select);
      modalApi.body.appendChild(label);

      const moveBtn = document.createElement('button');
      moveBtn.type = 'button';
      moveBtn.className = 'primary-btn';
      moveBtn.textContent = 'Move';
      moveBtn.addEventListener('click', async () => {
        const selectedSectionGid = String(select.value || '').trim();
        if (!selectedSectionGid) return;
        const moveUrl = asanaSectionAddTaskUrlForSection(selectedSectionGid);
        if (!moveUrl) {
          window.alert('Move endpoint not available.');
          return;
        }
        moveBtn.disabled = true;
        try {
          await postJson(moveUrl, { task_gid: taskGid });
          modalApi.close();
        } catch (error) {
          window.alert(`Unable to move task: ${String(error && error.message ? error.message : 'request_failed')}`);
        } finally {
          moveBtn.disabled = false;
        }
      });
      modalApi.actions.appendChild(moveBtn);
    }

    const cancelBtn = document.createElement('button');
    cancelBtn.type = 'button';
    cancelBtn.className = 'ghost-btn';
    cancelBtn.textContent = 'Cancel';
    cancelBtn.addEventListener('click', modalApi.close);
    modalApi.actions.appendChild(cancelBtn);
  };

  const openAsanaAgendaSections = (context) => {
    const view = String(context && context.view ? context.view : '').trim().toLowerCase();
    if (view !== 'agenda') return [];
    const activeFilter = String(context && context.activeFilter ? context.activeFilter : 'all').trim().toLowerCase();
    if (activeFilter !== 'all') return [];
    const includeCompletedWindow = true;
    outlookTeamsJoinByAgendaItemId.clear();

    const candidateRows = asanaTasks.filter((row) => {
      if (!row || typeof row !== 'object') return false;
      if (!Boolean(row.completed)) return true;
      return includeCompletedWindow && isTaskCompletedWithinWindow(row);
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
    asanaBoards.forEach((board) => {
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
        metaParts.push(completedAt ? `Completed ${formatTimelineWhen(completedAt)}` : 'Completed');
      } else {
        metaParts.push(dueDisplay ? `Due ${dueDisplay}` : 'No due date');
      }
      if (sectionName) {
        metaParts.push(sectionName);
      } else if (workspaceName) {
        metaParts.push(workspaceName);
      }

      const agendaItemId = `asana-agenda-${gid}`;
      const mappedResources = taskResourceNames(row, agendaItemId);
      if (mappedResources.length) {
        metaParts.push(`Resources: ${mappedResources.join(', ')}`);
      }

      const sortDate = completed ? (completedAt || '9999-12-31T23:59:59Z') : (dueDate || '9999-12-31');
      const sortTime = completed ? '99:99' : (dueTime || '99:99');
      const sortPrefix = completed ? '1' : '0';
      const sortDirection = completed ? `${9999999999999 - toEpoch(sortDate)}` : `${sortDate} ${sortTime}`;

      const itemBadges = [
        { id: 'attach-resources', label: 'Resources', title: 'Attach resources' },
      ];
      const assigneeName = String(row.assignee_name || '').trim();
      if (assigneeName) {
        itemBadges.push({ id: 'assignee', label: assigneeName, title: `Assigned to ${assigneeName}` });
      }
      const subtaskCount = Number(row.subtask_count || 0);
      if (subtaskCount > 0) {
        itemBadges.push({ id: 'subtasks', label: `${subtaskCount} subtask${subtaskCount !== 1 ? 's' : ''}`, title: `${subtaskCount} subtask${subtaskCount !== 1 ? 's' : ''}` });
      }

      const boards = asanaBoardRowsForTask(row);

      const item = {
        id: agendaItemId,
        title: String(row.name || '').trim() || `Asana task ${gid}`,
        meta: metaParts.join(' · '),
        date: dueDate,
        time: dueTime,
        source: 'asana',
        sourceItemId: gid,
        taskGid: gid,
        url: String(row.task_url || '').trim(),
        done: completed,
        canToggle: true,
        isExternal: true,
        badges: itemBadges,
        actions: [
          ...(boards.length ? [{ id: 'move-section', label: 'Move to section' }] : []),
          { id: 'delete', label: 'Delete' },
        ],
        sortKey: `${sortPrefix}-${sortDirection}-${String(row.name || '').trim().toLowerCase()}`,
      };

      if (!boards.length) {
        ensureBoardBucket({ name: 'Unassigned', gid: '' }).items.push(item);
        return;
      }
      boards.forEach((board) => {
        ensureBoardBucket(board).items.push(item);
      });
    });

    const asanaSections = Array.from(boardBuckets.values())
      .sort((left, right) => String(left.title || '').localeCompare(String(right.title || '')))
      .map((section) => ({
        ...section,
        emptyText: section.boardGid ? 'No tasks in this board.' : 'No tasks in this section.',
        items: section.items
          .slice()
          .sort((left, right) => String(left.sortKey || '').localeCompare(String(right.sortKey || '')))
          .map(({ sortKey, ...rest }) => rest),
      }));

    const selectedDate = String(context && context.selectedDate ? context.selectedDate : '').trim();
    let endDate = '';
    if (selectedDate) {
      const startDateObj = new Date(`${selectedDate}T00:00:00`);
      if (!Number.isNaN(startDateObj.getTime())) {
        startDateObj.setDate(startDateObj.getDate() + 20);
        endDate = toYmdLocal(startDateObj);
      }
    }

    const outlookItems = plannerExternalItems
      .filter((item) => plannerItemSource(item) === 'outlook')
      .filter((item) => {
        const itemDate = String(item && item.date ? item.date : '').trim();
        if (!itemDate) return false;
        if (!selectedDate || !endDate) return true;
        return itemDate >= selectedDate && itemDate <= endDate;
      })
      .map((item) => {
        const rawId = String(item && item.id ? item.id : '').trim();
        if (!rawId) return null;
        const agendaItemId = `outlook-agenda-${rawId}`;
        const teamsJoinUrl = outlookJoinUrlForItem(item);
        const eventUrl = outlookEventUrlForItem(item);
        const isTeamsMeeting = toBool(item && (item.is_teams_meeting || item.isTeamsMeeting))
          || (teamsJoinUrl ? teamsJoinUrl.toLowerCase().includes('teams.microsoft.com') : false);
        const dueDate = String(item && item.date ? item.date : '').trim();
        const dueTime = String(item && item.time ? item.time : '').trim();
        const status = String(item && item.status ? item.status : '').trim().toLowerCase();
        const metaParts = [];
        if (dueDate) {
          metaParts.push(dueTime ? `${dueDate} ${dueTime}` : dueDate);
        }
        if (isTeamsMeeting) {
          metaParts.push('Teams meeting');
        }
        if (status) {
          metaParts.push(status.charAt(0).toUpperCase() + status.slice(1));
        }
        const mappedResources = resourceNamesForUuids(agendaItemResourceUuids(agendaItemId));
        if (mappedResources.length) {
          metaParts.push(`Resources: ${mappedResources.join(', ')}`);
        }
        if (teamsJoinUrl) {
          outlookTeamsJoinByAgendaItemId.set(agendaItemId, teamsJoinUrl);
        }
        const actions = [];
        if (teamsJoinUrl) {
          actions.push({ id: 'join-teams', label: 'Join Teams' });
        }
        return {
          id: agendaItemId,
          title: String(item && item.title ? item.title : '').trim() || 'Outlook event',
          meta: metaParts.join(' · '),
          date: dueDate,
          time: dueTime,
          source: 'outlook',
          sourceItemId: rawId,
          url: eventUrl || teamsJoinUrl,
          done: Boolean(item && item.done),
          canToggle: false,
          isExternal: false,
          badges: [
            { id: 'attach-resources', label: 'Resources', title: 'Attach resources' },
          ],
          actions,
          sortKey: `${dueDate || '9999-12-31'} ${dueTime || '99:99'}-${String(item && item.title ? item.title : '').trim().toLowerCase()}`,
        };
      })
      .filter(Boolean)
      .sort((left, right) => String(left.sortKey || '').localeCompare(String(right.sortKey || '')))
      .map(({ sortKey, ...rest }) => rest);

    if (!outlookItems.length) return asanaSections;

    return [
      ...asanaSections,
      {
        id: 'outlook-calendar',
        title: 'Outlook - Calendar',
        description: 'Upcoming Outlook events and Teams meetings.',
        emptyText: 'No Outlook events in this range.',
        items: outlookItems,
      },
    ];
  };

  const handleAsanaToggleError = (item, error) => {
    const taskName = String(item && item.title ? item.title : 'task');
    const errorMessage = String(error && error.message ? error.message : 'update_failed');
    console.warn(`Unable to update ${taskName}: ${errorMessage}`);
    if (String(item && item.source ? item.source : '').toLowerCase() === 'asana') {
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
      return;
    }
    if (typeof window.alert === 'function') {
      window.alert(`Unable to update "${taskName}": ${errorMessage}`);
    }
  };

  if (plannerRoot && window.AlshivalPlanner && typeof window.AlshivalPlanner.init === 'function') {
    stripLegacyPlannerSeeds();
    plannerController = window.AlshivalPlanner.init(plannerRoot, {
      storageKey: plannerStorageKey,
      seedItems: () => [],
      externalItems: () => plannerExternalItems,
      agendaSections: openAsanaAgendaSections,
      onExternalToggle: async (item, done) => {
        if (plannerItemSource(item) !== 'asana') return;
        const gid = asanaTaskGidForItem(item);
        if (!gid) return;
        const payload = await updateAsanaTaskCompletion({ taskGid: gid }, done);
        syncAsanaTaskState(gid, done, String(payload && payload.completed_at ? payload.completed_at : ''));
        const agendaItemId = `asana-agenda-${gid}`;
        const mappedResources = agendaItemResourceUuids(agendaItemId);
        if (mappedResources.length) {
          const refreshedTask = asanaTaskRowByGid(gid);
          const agendaItem = {
            id: agendaItemId,
            source: 'asana',
            sourceItemId: gid,
            title: String(
              refreshedTask && refreshedTask.name
                ? refreshedTask.name
                : item && item.title
                  ? item.title
                  : `Asana task ${gid}`
            ).trim(),
            date: String(refreshedTask && refreshedTask.due_date ? refreshedTask.due_date : item && item.date ? item.date : '').trim(),
            time: String(refreshedTask && refreshedTask.due_time ? refreshedTask.due_time : item && item.time ? item.time : '').trim(),
            url: String(refreshedTask && refreshedTask.task_url ? refreshedTask.task_url : item && item.url ? item.url : '').trim(),
            done: Boolean(done),
          };
          try {
            const saved = await saveAgendaItemResourceMapping(agendaItem, mappedResources);
            syncAgendaItemMapping(agendaItemId, saved);
          } catch (error) {
            console.warn(`Unable to sync resource task completion for ${agendaItemId}`);
          }
        }
      },
      onAgendaSectionAction: async (item, actionId, section) => {
        const normalizedAction = String(actionId || '').trim().toLowerCase();
        if (normalizedAction === 'join-teams') {
          const itemId = String(item && item.id ? item.id : '').trim();
          const teamsJoinUrl = String(outlookTeamsJoinByAgendaItemId.get(itemId) || '').trim();
          const fallbackUrl = String(item && item.url ? item.url : '').trim();
          const destinationUrl = teamsJoinUrl || fallbackUrl;
          if (destinationUrl) {
            window.open(destinationUrl, '_blank', 'noopener,noreferrer');
          }
          return;
        }
        if (normalizedAction === 'create-task') {
          await openAsanaCreateTaskModal(section || {});
          return;
        }
        if (normalizedAction === 'delete') {
          await openAsanaDeleteTaskModal(item);
          return;
        }
        if (normalizedAction === 'attach-resources') {
          await openAgendaResourceMappingModal(item);
          return;
        }
        if (normalizedAction === 'move-section') {
          await openAsanaMoveToSectionModal(item, section || {});
        }
      },
      onItemClick: (item) => {
        if (plannerItemSource(item) === 'asana') {
          openAsanaTaskDrawer(item);
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
      monthEmptyText: 'No agenda items for this day.',
      listEmptyText: 'No upcoming items in this range.',
      listWindowDays: 21,
    });
  }

  const askMessages = dashboard.querySelector('[data-ask-messages]');
  const askForm = dashboard.querySelector('[data-ask-form]');
  const askInput = dashboard.querySelector('[data-ask-input]');
  const askSend = dashboard.querySelector('[data-ask-send]');
  const askSudo = dashboard.querySelector('[data-ask-sudo]');
  const chatUrl = dashboard.getAttribute('data-chat-url') || '';

  let askPending = false;
  let askInitialized = false;
  let conversationId = '';
  try {
    const saved = window.localStorage.getItem('overview_ask_conversation_id') || '';
    conversationId = saved || `overview-${Date.now()}`;
  } catch (error) {
    conversationId = `overview-${Date.now()}`;
  }

  const addAskMessage = (role, text) => {
    if (!askMessages) return;
    const node = document.createElement('div');
    node.className = `ask-chat-msg ${role === 'user' ? 'ask-chat-msg--user' : 'ask-chat-msg--assistant'}`;
    node.textContent = String(text || '').trim();
    askMessages.appendChild(node);
    if (!askInitialized) {
      askMessages.scrollTop = 0;
      askInitialized = true;
    }
  };

  const setAskPending = (next) => {
    askPending = Boolean(next);
    if (askSend) askSend.disabled = askPending;
    if (askInput) askInput.disabled = askPending;
  };

  if (askInput && askForm) {
    askInput.addEventListener('keydown', (event) => {
      if (event.key !== 'Enter' || event.shiftKey || event.isComposing) return;
      event.preventDefault();
      if (typeof askForm.requestSubmit === 'function') {
        askForm.requestSubmit();
      } else {
        askForm.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
      }
    });
  }

  if (askForm && askInput && chatUrl) {
    askForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      if (askPending) return;
      const message = String(askInput.value || '').trim();
      if (!message) return;

      addAskMessage('user', message);
      askInput.value = '';
      setAskPending(true);

      try {
        const response = await fetch(chatUrl, {
          method: 'POST',
          credentials: 'same-origin',
          headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCookie('csrftoken'),
            'X-Requested-With': 'XMLHttpRequest',
          },
          body: JSON.stringify({
            message,
            conversation_id: conversationId,
          }),
        });

        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
          addAskMessage('assistant', `Chat unavailable (${String(payload.error || 'request_failed')}).`);
          return;
        }

        const nextConversationId = String(payload.conversation_id || '').trim();
        if (nextConversationId) {
          conversationId = nextConversationId;
          try {
            window.localStorage.setItem('overview_ask_conversation_id', conversationId);
          } catch (error) {
            // Ignore storage write issues.
          }
        }
        addAskMessage('assistant', String(payload.reply || '').trim() || 'No response.');
      } catch (error) {
        addAskMessage('assistant', 'Chat unavailable right now.');
      } finally {
        setAskPending(false);
        askInput.focus();
      }
    });
  }

  if (askSudo) {
    askSudo.addEventListener('click', async (event) => {
      event.preventDefault();
      const isSuperuser = dashboard.getAttribute('data-superuser') === '1';
      if (!isSuperuser) {
        return;
      }

      if (typeof window.openAskAlshivalWidget === 'function') {
        await window.openAskAlshivalWidget({
          mode: 'shell',
          title: 'System Terminal',
          hintText: 'Superuser host login shell',
        });
        return;
      }

      addAskMessage('assistant', 'Sudo mode terminal is unavailable on this page.');
    });
  }

  const calendarAlertOpenButton = dashboard.querySelector('[data-calendar-alert-open]');
  const calendarAlertModal = document.querySelector('[data-calendar-alert-modal]');

  const closeCalendarAlertModal = () => {
    if (!calendarAlertModal) return;
    calendarAlertModal.classList.remove('is-active');
    calendarAlertModal.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('modal-open');
  };

  const openCalendarAlertModal = () => {
    if (!calendarAlertModal) return;
    calendarAlertModal.classList.add('is-active');
    calendarAlertModal.setAttribute('aria-hidden', 'false');
    document.body.classList.add('modal-open');
  };

  if (calendarAlertOpenButton && calendarAlertModal) {
    calendarAlertOpenButton.addEventListener('click', openCalendarAlertModal);
    calendarAlertModal.querySelectorAll('[data-calendar-alert-close]').forEach((el) => {
      el.addEventListener('click', closeCalendarAlertModal);
    });
    document.addEventListener('keydown', (event) => {
      if (event.key !== 'Escape') return;
      if (!calendarAlertModal.classList.contains('is-active')) return;
      closeCalendarAlertModal();
    });
    if (String(window.location.hash || '') === '#calendar-alerts') {
      openCalendarAlertModal();
    }
  }

  const notifyList = dashboard.querySelector('[data-notify-list]');
  const notifyUnread = dashboard.querySelector('[data-notify-unread]');
  const notifyMarkRead = dashboard.querySelector('[data-notify-mark-read]');
  const notifyClear = dashboard.querySelector('[data-notify-clear]');
  const notificationsUrl = dashboard.getAttribute('data-notifications-url') || '';
  const notificationsMarkReadUrl = dashboard.getAttribute('data-notifications-mark-read-url') || '';
  const notificationsClearUrl = dashboard.getAttribute('data-notifications-clear-url') || '';

  const clearNotifyList = () => {
    if (!notifyList) return;
    while (notifyList.firstChild) {
      notifyList.removeChild(notifyList.firstChild);
    }
  };

  const renderNotifyEmpty = (message) => {
    if (!notifyList) return;
    clearNotifyList();
    const empty = document.createElement('p');
    empty.className = 'notification-empty';
    empty.textContent = message;
    notifyList.appendChild(empty);
  };

  const formatWhen = (value) => {
    if (!value) return '';
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
  };

  const setUnread = (count) => {
    if (!notifyUnread) return;
    const value = Number.isFinite(Number(count)) ? Math.max(0, Number(count)) : 0;
    notifyUnread.textContent = `${value} unread`;
  };

  const renderNotifications = (items) => {
    if (!notifyList) return;
    clearNotifyList();
    if (!Array.isArray(items) || items.length === 0) {
      renderNotifyEmpty('No alerts yet.');
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
      notifyList.appendChild(card);
    });
  };

  const loadNotifications = async () => {
    if (!notificationsUrl) return;
    try {
      const response = await fetch(`${notificationsUrl}?limit=8`, {
        method: 'GET',
        credentials: 'same-origin',
      });
      if (!response.ok) {
        throw new Error(`notifications_fetch_${response.status}`);
      }
      const payload = await response.json();
      const unread = Number(payload && payload.unread_count ? payload.unread_count : 0);
      const items = payload && Array.isArray(payload.items) ? payload.items : [];
      setUnread(unread);
      renderNotifications(items);
    } catch (error) {
      renderNotifyEmpty('Unable to load notifications.');
    }
  };

  const markAllNotificationsRead = async () => {
    if (!notificationsMarkReadUrl) return;
    try {
      const response = await fetch(notificationsMarkReadUrl, {
        method: 'POST',
        credentials: 'same-origin',
        headers: {
          'X-CSRFToken': getCookie('csrftoken'),
        },
      });
      if (!response.ok) return;
      await loadNotifications();
    } catch (error) {
      // Keep controls interactive when request fails.
    }
  };

  const clearAllNotifications = async () => {
    if (!notificationsClearUrl) return;
    try {
      const response = await fetch(notificationsClearUrl, {
        method: 'POST',
        credentials: 'same-origin',
        headers: {
          'X-CSRFToken': getCookie('csrftoken'),
        },
      });
      if (!response.ok) return;
      setUnread(0);
      renderNotifyEmpty('No alerts yet.');
    } catch (error) {
      // Keep controls interactive when request fails.
    }
  };

  if (notifyMarkRead) {
    notifyMarkRead.addEventListener('click', async () => {
      await markAllNotificationsRead();
    });
  }

  if (notifyClear) {
    notifyClear.addEventListener('click', async () => {
      await clearAllNotifications();
    });
  }

  loadNotifications();
})();
