const state = {
  subtype: "cpp_python",
  samples: [],
  sample: null,
  selectedFilePath: "",
  dragMode: null,
  fileSearch: "",
  fileFilter: "all",
  autoRefreshTimer: null,
};

const AUTO_REFRESH_MS = 15000;

const els = {
  subtypeSelect: document.getElementById("subtypeSelect"),
  sampleCount: document.getElementById("sampleCount"),
  sampleList: document.getElementById("sampleList"),
  sampleTitle: document.getElementById("sampleTitle"),
  sampleSubtitle: document.getElementById("sampleSubtitle"),
  heroStats: document.getElementById("heroStats"),
  overviewGrid: document.getElementById("overviewGrid"),
  bodyBlock: document.getElementById("bodyBlock"),
  copyBodyBtn: document.getElementById("copyBodyBtn"),
  leftGuideList: document.getElementById("leftGuideList"),
  rightGuideList: document.getElementById("rightGuideList"),
  instanceBadge: document.getElementById("instanceBadge"),
  fileList: document.getElementById("fileList"),
  fileSearchInput: document.getElementById("fileSearchInput"),
  fileFilterSelect: document.getElementById("fileFilterSelect"),
  fileMeta: document.getElementById("fileMeta"),
  fileDetailGrid: document.getElementById("fileDetailGrid"),
  patchPreview: document.getElementById("patchPreview"),
  r0Content: document.getElementById("r0Content"),
  rnContent: document.getElementById("rnContent"),
  comparePane: document.getElementById("comparePane"),
  compareSplitter: document.getElementById("compareSplitter"),
  saveStatus: document.getElementById("saveStatus"),
  reloadBtn: document.getElementById("reloadBtn"),
  saveBtn: document.getElementById("saveBtn"),
  fillExampleBtn: document.getElementById("fillExampleBtn"),
  openDetailsModalBtn: document.getElementById("openDetailsModalBtn"),
  openPatchModalBtn: document.getElementById("openPatchModalBtn"),
  openCompareModalBtn: document.getElementById("openCompareModalBtn"),
  copyAllPatchesBtn: document.getElementById("copyAllPatchesBtn"),
  detailModal: document.getElementById("detailModal"),
  detailModalTitle: document.getElementById("detailModalTitle"),
  detailModalBody: document.getElementById("detailModalBody"),
  copyDetailModalBtn: document.getElementById("copyDetailModalBtn"),
  closeDetailModalBtn: document.getElementById("closeDetailModalBtn"),
  copyFallbackModal: document.getElementById("copyFallbackModal"),
  closeCopyFallbackBtn: document.getElementById("closeCopyFallbackBtn"),
  copyFallbackTextarea: document.getElementById("copyFallbackTextarea"),
  sourceLanguage: document.getElementById("sourceLanguage"),
  targetLanguage: document.getElementById("targetLanguage"),
  sourceVersion: document.getElementById("sourceVersion"),
  targetVersion: document.getElementById("targetVersion"),
  migrationPattern: document.getElementById("migrationPattern"),
  testFramework: document.getElementById("testFramework"),
  buildSystem: document.getElementById("buildSystem"),
  reproducible: document.getElementById("reproducible"),
  issueRewriteReady: document.getElementById("issueRewriteReady"),
  leakageRisk: document.getElementById("leakageRisk"),
  excludeReason: document.getElementById("excludeReason"),
  reviewer: document.getElementById("reviewer"),
  crossCheckStatus: document.getElementById("crossCheckStatus"),
  notes: document.getElementById("notes"),
};

const options = {
  manual_label: [
    ["positive", "positive / 正例"],
    ["negative", "negative / 负例"],
    ["uncertain", "uncertain / 不确定"],
  ],
  implementation_scope: [
    ["partial_feature_migration", "partial_feature_migration / 部分功能迁移"],
    ["full_repo_translation", "full_repo_translation / 整仓翻译"],
    ["not_applicable", "not_applicable / 不适用"],
  ],
  logic_equivalence_scope: [
    ["same_logic_translation", "same_logic_translation / 同一逻辑翻译"],
    ["partial_logic_replacement", "partial_logic_replacement / 部分逻辑替换"],
    ["unclear_logic_mapping", "unclear_logic_mapping / 逻辑不清"],
  ],
  languages: [
    ["python", "python / Python"],
    ["c++", "c++ / C/C++"],
    ["java", "java / Java"],
  ],
  reproducible: [
    ["unknown", "unknown / 待确认"],
    ["yes", "yes / 可复现"],
    ["no", "no / 不可复现"],
  ],
  issue_rewrite_ready: [
    ["needs_check", "needs_check / 还需检查"],
    ["yes", "yes / 可直接改写 issue"],
    ["no", "no / 暂不适合"],
  ],
  leakage_risk: [
    ["low", "low / 低"],
    ["medium", "medium / 中"],
    ["high", "high / 高"],
  ],
  cross_check_status: [
    ["pending", "pending / 待复核"],
    ["checked", "checked / 已复核"],
    ["disagreed", "disagreed / 有分歧"],
  ],
};

function qs(name) {
  return document.querySelector(name);
}

function createRadioGroup(containerId, name, entries) {
  const container = document.getElementById(containerId);
  container.innerHTML = "";
  entries.forEach(([value, label]) => {
    const labelEl = document.createElement("label");
    labelEl.className = "option";
    const input = document.createElement("input");
    input.type = "radio";
    input.name = name;
    input.value = value;
    labelEl.appendChild(input);
    labelEl.append(label);
    container.appendChild(labelEl);
  });
}

function fillSelect(selectEl, entries) {
  selectEl.innerHTML = "";
  entries.forEach(([value, label]) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    selectEl.appendChild(option);
  });
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `请求失败：${response.status}`);
  }
  return response.json();
}

function setSaveStatus(text, type = "") {
  els.saveStatus.textContent = text;
  els.saveStatus.className = `save-status ${type}`.trim();
}

async function copyText(text, successMessage) {
  const normalized = text || "";
  if (!normalized) {
    setSaveStatus("?????????", "error");
    return false;
  }

  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(normalized);
      setSaveStatus(successMessage || "????????", "success");
      return true;
    }
  } catch (error) {
    // continue to legacy fallback
  }

  const textarea = document.createElement("textarea");
  textarea.value = normalized;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  textarea.style.top = "0";
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  textarea.setSelectionRange(0, textarea.value.length);

  try {
    const copied = document.execCommand("copy");
    document.body.removeChild(textarea);
    if (copied) {
      setSaveStatus(successMessage || "????????", "success");
      return true;
    }
  } catch (error) {
    document.body.removeChild(textarea);
  }

  openCopyFallback(normalized);
  setSaveStatus("?????????????????", "error");
  return false;
}

function heroStat(label, value) {
  return `<div class="hero-stat"><span class="k">${label}</span><span class="v">${value || "—"}</span></div>`;
}

function getRadioValue(name) {
  const checked = document.querySelector(`input[name="${name}"]:checked`);
  return checked ? checked.value : "";
}

function setRadioValue(name, value) {
  document.querySelectorAll(`input[name="${name}"]`).forEach((input) => {
    input.checked = input.value === value;
  });
}

function renderSampleList() {
  els.sampleList.innerHTML = "";
  els.sampleCount.textContent = `${state.samples.length} 条样本`;
  state.samples.forEach((item) => {
    const card = document.createElement("button");
    card.className = `sample-card ${state.sample?.instance_id === item.instance_id ? "is-active" : ""}`;
    card.type = "button";
    card.innerHTML = `
      <div class="title">${item.title || item.instance_id}</div>
      <div class="meta">
        <span>${item.repo_full_name}</span>
        <span>${item.manual_label_zh || "未标注"}</span>
      </div>
    `;
    card.addEventListener("click", () => loadSample(item.instance_id));
    els.sampleList.appendChild(card);
  });
}

function renderOverview(sample) {
  const meta = sample.metadata;
  const row = sample.row;
  els.sampleTitle.textContent = meta.title || sample.instance_id;
  els.sampleSubtitle.textContent = `${meta.repo_full_name} · PR #${row.pr_number} · ${sample.subtype_zh}`;
  els.instanceBadge.textContent = sample.instance_id;
  els.heroStats.innerHTML = [
    heroStat("仓库", meta.repo_full_name),
    heroStat("Stars", meta.repo_stars),
    heroStat("自动信号", (meta.auto_filter?.auto_signals || []).join(" / ")),
    heroStat("PR 创建时间", meta.pr_created_at),
  ].join("");

  const overviewItems = [
    ["迁移类型", row.migration_type_zh || row.migration_type || "—"],
    ["源语言 → 目标语言", `${row.source_language_zh || row.source_language || "—"} → ${row.target_language_zh || row.target_language || "—"}`],
    ["原始有测试", row.has_tests_before_zh || "—"],
    ["新增测试", row.adds_new_tests_zh || "—"],
    ["构建系统", row.build_system_zh || row.build_system || "—"],
    ["测试框架", row.test_framework_zh || row.test_framework || "—"],
  ];
  els.overviewGrid.innerHTML = overviewItems
    .map(([k, v]) => `<div class="info-chip"><span class="k">${k}</span><span class="v">${v || "—"}</span></div>`)
    .join("");

  els.bodyBlock.textContent = meta.body || "该 PR 没有正文说明。";

  els.leftGuideList.innerHTML = "";
  (sample.recommended_mapping.left_panel || []).forEach((line) => {
    const li = document.createElement("li");
    li.textContent = line;
    els.leftGuideList.appendChild(li);
  });
  els.rightGuideList.innerHTML = "";
  (sample.recommended_mapping.right_panel || []).forEach((line) => {
    const li = document.createElement("li");
    li.textContent = line;
    els.rightGuideList.appendChild(li);
  });
}

function renderFileList(sample) {
  els.fileList.innerHTML = "";
  const files = (sample.evidence_files || []).filter(matchesFileFilter);
  if (!files.length) {
    els.fileList.innerHTML = `<div class="file-item"><span class="path">没有可展示的相关文件。</span></div>`;
    return;
  }
  files.forEach((item, index) => {
    const isActive = state.selectedFilePath === item.path || (!state.selectedFilePath && index === 0);
    const el = document.createElement("button");
    el.type = "button";
    el.className = `file-item ${isActive ? "is-active" : ""} ${isDiffCandidate(item) ? "is-diff" : ""}`.trim();
    const badges = buildFileBadges(item);
    el.innerHTML = `
      <span class="path">${item.path}</span>
      <div class="meta">${item.status} · +${item.additions} / -${item.deletions} · r0:${item.exists_r0 ? "有" : "无"} · rn:${item.exists_rn ? "有" : "无"}</div>
      <div class="badges">${badges}</div>
    `;
    el.addEventListener("click", () => selectFile(item.path));
    els.fileList.appendChild(el);
  });
  if ((!state.selectedFilePath || !files.some((item) => item.path === state.selectedFilePath)) && files[0]) {
    state.selectedFilePath = files[0].path;
  }
}

function renderFileDetail(file) {
  const details = [
    ["文件路径", file.path],
    ["状态", file.status],
    ["总改动数", String(file.changes)],
    ["新增 / 删除", `+${file.additions} / -${file.deletions}`],
    ["r0 是否存在", file.exists_r0 ? "有" : "无"],
    ["rn 是否存在", file.exists_rn ? "有" : "无"],
    ["Patch 可用", file.has_patch ? "是" : "否"],
    ["差异阅读建议", isDiffCandidate(file) ? "优先阅读：这类文件更可能直接支持标注判断" : "可作为补充证据阅读"],
  ];
  els.fileDetailGrid.innerHTML = details
    .map(([k, v], index) => `<div class="detail-chip ${index >= 6 ? "detail-chip-wide" : ""}"><span class="k">${k}</span><span class="v">${v}</span></div>`)
    .join("");
}

async function selectFile(path) {
  if (!state.sample) return;
  state.selectedFilePath = path;
  renderFileList(state.sample);
  const file = state.sample.evidence_files.find((item) => item.path === path);
  if (!file) return;

  els.fileMeta.textContent = `${file.path} · 状态: ${file.status} · changes=${file.changes} · r0=${file.exists_r0 ? "有" : "无"} · rn=${file.exists_rn ? "有" : "无"}`;
  renderFileDetail(file);
  els.patchPreview.textContent = file.patch_preview || "该文件没有可用 patch 片段。";

  const [r0, rn] = await Promise.all([
    fetchJson(`/api/file?instance_id=${encodeURIComponent(state.sample.instance_id)}&side=r0&path=${encodeURIComponent(path)}`),
    fetchJson(`/api/file?instance_id=${encodeURIComponent(state.sample.instance_id)}&side=rn&path=${encodeURIComponent(path)}`),
  ]);

  els.r0Content.textContent = r0.content || "[r0 中没有该文件]";
  els.rnContent.textContent = rn.content || "[rn 中没有该文件]";
}

function setHelpText(sample) {
  const guides = sample.field_guides || {};
  const mappings = {
    help_manual_label: "manual_label",
    help_implementation_scope: "implementation_scope",
    help_logic_equivalence_scope: "logic_equivalence_scope",
    help_source_target: "source_target",
    help_migration_pattern: "migration_pattern",
    help_reproducible: "reproducible",
    help_issue_rewrite_ready: "issue_rewrite_ready",
    help_leakage_risk: "leakage_risk",
    help_exclude_reason: "exclude_reason",
    help_notes: "notes",
  };
  Object.entries(mappings).forEach(([id, key]) => {
    const el = document.getElementById(id);
    if (el) el.textContent = guides[key] || "";
  });
}

function fillForm(sample) {
  const saved = sample.saved_annotation || {};
  const row = sample.row || {};
  setHelpText(sample);

  setRadioValue("manual_label", saved.manual_label || row.manual_label || "");
  setRadioValue("implementation_scope", saved.implementation_scope || row.implementation_scope || "");
  setRadioValue("logic_equivalence_scope", saved.logic_equivalence_scope || row.logic_equivalence_scope || "");
  els.sourceLanguage.value = saved.source_language || row.source_language || "";
  els.targetLanguage.value = saved.target_language || row.target_language || "";
  els.sourceVersion.value = saved.source_version || row.source_version || "";
  els.targetVersion.value = saved.target_version || row.target_version || "";
  els.migrationPattern.value = saved.migration_pattern || row.migration_pattern || "";
  els.testFramework.value = saved.test_framework || row.test_framework || "";
  els.buildSystem.value = saved.build_system || row.build_system || "";
  els.reproducible.value = saved.reproducible || row.reproducible || "unknown";
  els.issueRewriteReady.value = saved.issue_rewrite_ready || row.issue_rewrite_ready || "needs_check";
  els.leakageRisk.value = saved.leakage_risk || row.leakage_risk || "medium";
  els.excludeReason.value = saved.exclude_reason || row.exclude_reason || "";
  els.reviewer.value = saved.reviewer || row.reviewer || "";
  els.crossCheckStatus.value = saved.cross_check_status || row.cross_check_status || "pending";
  els.notes.value = saved.notes || row.notes || "";
}

function collectPayload() {
  return {
    instance_id: state.sample.instance_id,
    subtype: state.sample.subtype,
    manual_label: getRadioValue("manual_label"),
    implementation_scope: getRadioValue("implementation_scope"),
    logic_equivalence_scope: getRadioValue("logic_equivalence_scope"),
    source_language: els.sourceLanguage.value,
    target_language: els.targetLanguage.value,
    source_version: els.sourceVersion.value.trim(),
    target_version: els.targetVersion.value.trim(),
    migration_pattern: els.migrationPattern.value.trim(),
    test_framework: els.testFramework.value.trim(),
    build_system: els.buildSystem.value.trim(),
    reproducible: els.reproducible.value,
    issue_rewrite_ready: els.issueRewriteReady.value,
    leakage_risk: els.leakageRisk.value,
    exclude_reason: els.excludeReason.value.trim(),
    reviewer: els.reviewer.value.trim(),
    cross_check_status: els.crossCheckStatus.value,
    notes: els.notes.value.trim(),
  };
}

async function saveAnnotation() {
  if (!state.sample) return;
  try {
    const payload = collectPayload();
    const response = await fetchJson("/api/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setSaveStatus(`已保存：${response.saved.instance_id} · ${response.saved.saved_at}`, "success");
    state.sample.saved_annotation = response.saved;
    renderSampleList();
  } catch (error) {
    setSaveStatus(`保存失败：${error.message}`, "error");
  }
}

function fillExample() {
  if (!state.sample) return;
  const subtype = state.sample.subtype;
  const title = state.sample.metadata?.title || "";
  const body = state.sample.metadata?.body || "";
  const strongPositive = /ported from|rewrite .* in python|replace .* with .*|ported to/i.test(`${title}\n${body}`);

  setRadioValue("manual_label", strongPositive ? "positive" : "uncertain");
  setRadioValue("implementation_scope", subtype === "py2_py3" ? "not_applicable" : "partial_feature_migration");
  setRadioValue("logic_equivalence_scope", strongPositive ? "same_logic_translation" : "unclear_logic_mapping");
  els.reproducible.value = "unknown";
  els.issueRewriteReady.value = "needs_check";
  els.leakageRisk.value = strongPositive ? "medium" : "low";
  els.crossCheckStatus.value = "pending";
  if (!els.notes.value.trim()) {
    els.notes.value = "根据 PR 标题、正文与改动文件判断：这是一个需要重点核对新旧逻辑对应关系的迁移样本。";
  }
}

function openModal(mode) {
  els.detailModal.hidden = false;
  const file = state.sample?.evidence_files?.find((item) => item.path === state.selectedFilePath);
  els.copyDetailModalBtn.style.display = "inline-flex";
  if (mode === "details") {
    els.detailModalTitle.textContent = "文件详情放大查看";
    if (!file) {
      els.detailModalBody.innerHTML = `<div class="save-status">请先选择一个文件。</div>`;
      els.copyDetailModalBtn.onclick = null;
      return;
    }
    const detailRows = [
      ["文件路径", file.path],
      ["状态", file.status],
      ["总改动数", String(file.changes)],
      ["新增 / 删除", `+${file.additions} / -${file.deletions}`],
      ["r0 是否存在", file.exists_r0 ? "有" : "无"],
      ["rn 是否存在", file.exists_rn ? "有" : "无"],
      ["Patch 可用", file.has_patch ? "是" : "否"],
      ["差异阅读建议", isDiffCandidate(file) ? "优先阅读：这类文件更可能直接支持标注判断" : "可作为补充证据阅读"],
    ];
    els.detailModalBody.innerHTML = `
      <div class="modal-detail-grid">
        ${detailRows.map(([k, v], index) => `
          <div class="detail-chip ${index >= 6 ? "detail-chip-wide" : ""}">
            <span class="k">${escapeHtml(k)}</span>
            <span class="v">${escapeHtml(v)}</span>
          </div>
        `).join("")}
      </div>
    `;
    els.copyDetailModalBtn.onclick = () => {
      const text = detailRows.map(([k, v]) => `${k}: ${v}`).join("\n");
      copyText(text, "文件详情已复制到剪贴板。");
    };
    return;
  }
  if (mode === "patch") {
    els.detailModalTitle.textContent = "Patch 放大查看";
    els.detailModalBody.innerHTML = `
      <pre class="code-view">${escapeHtml(els.patchPreview.textContent)}</pre>
    `;
    els.copyDetailModalBtn.onclick = () => {
      copyText(els.patchPreview.textContent, "Patch 已复制到剪贴板。");
    };
    return;
  }
  els.detailModalTitle.textContent = "r0 / rn 对照放大查看";
  els.detailModalBody.innerHTML = `
    <div class="modal-compare">
      <section class="snapshot-pane">
        <div class="box-title">r0 改动前</div>
        <pre class="code-view">${escapeHtml(els.r0Content.textContent)}</pre>
      </section>
      <section class="snapshot-pane">
        <div class="box-title">rn 改动后</div>
        <pre class="code-view">${escapeHtml(els.rnContent.textContent)}</pre>
      </section>
    </div>
  `;
  els.copyDetailModalBtn.onclick = () => {
    const text = `r0 内容：\n${els.r0Content.textContent}\n\nrn 内容：\n${els.rnContent.textContent}`;
    copyText(text, "r0 / rn 对照内容已复制到剪贴板。");
  };
}

function closeModal() {
  els.detailModal.hidden = true;
  els.copyDetailModalBtn.onclick = null;
}

function openCopyFallback(text) {
  els.copyFallbackTextarea.value = text || "";
  els.copyFallbackModal.hidden = false;
  requestAnimationFrame(() => {
    els.copyFallbackTextarea.focus();
    els.copyFallbackTextarea.select();
    els.copyFallbackTextarea.setSelectionRange(0, els.copyFallbackTextarea.value.length);
  });
}

function closeCopyFallback() {
  els.copyFallbackModal.hidden = true;
}

function escapeHtml(text) {
  return (text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function setupAccordion() {
  document.querySelectorAll(".stack-toggle").forEach((button) => {
    button.addEventListener("click", () => {
      const card = button.closest(".stack-card");
      card.classList.toggle("is-open");
    });
  });
}

function isDiffCandidate(file) {
  const path = (file.path || "").toLowerCase();
  const codeLike = [".py", ".cpp", ".cc", ".c", ".h", ".hpp", ".java"].some((suffix) => path.endsWith(suffix));
  return codeLike && file.has_patch && (file.exists_r0 || file.exists_rn);
}

function buildFileBadges(file) {
  const path = (file.path || "").toLowerCase();
  const badges = [];
  if (isDiffCandidate(file)) badges.push(`<span class="mini-badge diff">差异候选</span>`);
  if (path.includes("test")) badges.push(`<span class="mini-badge test">测试</span>`);
  if (path.endsWith(".py")) badges.push(`<span class="mini-badge">Python</span>`);
  if ([".cpp", ".cc", ".c", ".h", ".hpp"].some((suffix) => path.endsWith(suffix))) badges.push(`<span class="mini-badge">C/C++</span>`);
  if (path.endsWith(".java")) badges.push(`<span class="mini-badge">Java</span>`);
  return badges.join("");
}

function matchesFileFilter(file) {
  const path = (file.path || "").toLowerCase();
  const query = state.fileSearch.trim().toLowerCase();
  if (query && !path.includes(query)) return false;
  switch (state.fileFilter) {
    case "diff":
      return isDiffCandidate(file);
    case "python":
      return path.endsWith(".py") || path.endsWith(".pyi");
    case "cpp":
      return [".cpp", ".cc", ".c", ".h", ".hpp", ".hh", ".cxx", ".hxx"].some((suffix) => path.endsWith(suffix));
    case "java":
      return path.endsWith(".java");
    case "tests":
      return path.includes("test");
    default:
      return true;
  }
}

function setupSplitter() {
  if (!els.compareSplitter || !els.comparePane) return;
  els.compareSplitter.addEventListener("mousedown", (event) => {
    state.dragMode = {
      startX: event.clientX,
      paneWidth: els.comparePane.getBoundingClientRect().width,
      leftWidth: document.getElementById("compareLeft").getBoundingClientRect().width,
    };
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
  });

  window.addEventListener("mousemove", (event) => {
    if (!state.dragMode) return;
    const delta = event.clientX - state.dragMode.startX;
    const nextLeft = Math.max(240, Math.min(state.dragMode.paneWidth - 260, state.dragMode.leftWidth + delta));
    els.comparePane.style.gridTemplateColumns = `${nextLeft}px 12px minmax(240px, 1fr)`;
  });

  window.addEventListener("mouseup", () => {
    if (!state.dragMode) return;
    state.dragMode = null;
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
  });
}

async function loadSample(instanceId) {
  try {
    state.sample = await fetchJson(`/api/sample/${encodeURIComponent(instanceId)}`);
    state.selectedFilePath = "";
    renderSampleList();
    renderOverview(state.sample);
    renderFileList(state.sample);
    fillForm(state.sample);
    const firstFile = state.sample.evidence_files[0];
    if (firstFile) {
      await selectFile(firstFile.path);
    } else {
      els.fileDetailGrid.innerHTML = "";
    }
    setSaveStatus("样本已加载，可以开始标注。");
  } catch (error) {
    setSaveStatus(`样本加载失败：${error.message}`, "error");
  }
}

async function loadSamples() {
  const previousId = state.sample?.instance_id || "";
  state.samples = await fetchJson(`/api/samples?subtype=${encodeURIComponent(state.subtype)}`);
  renderSampleList();
  if (state.samples[0]) {
    const matched = previousId && state.samples.find((item) => item.instance_id === previousId);
    await loadSample((matched || state.samples[0]).instance_id);
  } else {
    state.sample = null;
    setSaveStatus("??????????? snapshot ???", "error");
  }
}

function startAutoRefresh() {
  if (state.autoRefreshTimer) {
    window.clearInterval(state.autoRefreshTimer);
  }
  state.autoRefreshTimer = window.setInterval(async () => {
    if (document.hidden) return;
    try {
      const previousCount = state.samples.length;
      const previousId = state.sample?.instance_id || "";
      const samples = await fetchJson(`/api/samples?subtype=${encodeURIComponent(state.subtype)}`);
      state.samples = samples;
      renderSampleList();
      if (!samples.length) {
        state.sample = null;
        return;
      }
      const stillExists = previousId && samples.some((item) => item.instance_id === previousId);
      if (!stillExists) {
        await loadSample(samples[0].instance_id);
        setSaveStatus(`?????????? ${samples.length} ????`, "success");
        return;
      }
      if (samples.length !== previousCount) {
        setSaveStatus(`??? snapshot ???????? ${previousCount} ?? ${samples.length}?`, "success");
      }
    } catch (error) {
      setSaveStatus(`???????${error.message}`, "error");
    }
  }, AUTO_REFRESH_MS);
}

async function init() {
  createRadioGroup("manualLabelGroup", "manual_label", options.manual_label);
  createRadioGroup("implementationScopeGroup", "implementation_scope", options.implementation_scope);
  createRadioGroup("logicScopeGroup", "logic_equivalence_scope", options.logic_equivalence_scope);

  fillSelect(els.sourceLanguage, options.languages);
  fillSelect(els.targetLanguage, options.languages);
  fillSelect(els.reproducible, options.reproducible);
  fillSelect(els.issueRewriteReady, options.issue_rewrite_ready);
  fillSelect(els.leakageRisk, options.leakage_risk);
  fillSelect(els.crossCheckStatus, options.cross_check_status);

  const subtypes = await fetchJson("/api/subtypes");
  els.subtypeSelect.innerHTML = "";
  subtypes.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = `${item.label} (${item.value})`;
    els.subtypeSelect.appendChild(option);
  });
  els.subtypeSelect.value = state.subtype;

  els.subtypeSelect.addEventListener("change", async (event) => {
    state.subtype = event.target.value;
    await loadSamples();
  });
  els.reloadBtn.addEventListener("click", loadSamples);
  els.saveBtn.addEventListener("click", saveAnnotation);
  els.fillExampleBtn.addEventListener("click", fillExample);
  els.copyBodyBtn.addEventListener("click", () => {
    const title = state.sample?.metadata?.title || "";
    const body = els.bodyBlock.textContent || "";
    const payload = `PR 标题：${title}\n\nPR 摘要：\n${body}`;
    copyText(payload, "PR 摘要已复制到剪贴板。");
  });
  els.openDetailsModalBtn.addEventListener("click", () => openModal("details"));
  els.openPatchModalBtn.addEventListener("click", () => openModal("patch"));
  els.openCompareModalBtn.addEventListener("click", () => openModal("compare"));
  els.copyAllPatchesBtn.addEventListener("click", async () => {
    if (!state.sample) {
      setSaveStatus("请先选择一个样本。", "error");
      return;
    }
    try {
      const response = await fetchJson(`/api/all-patches?instance_id=${encodeURIComponent(state.sample.instance_id)}`);
      await copyText(response.content, "当前样本的全部 Patch 已复制到剪贴板。");
    } catch (error) {
      setSaveStatus(`复制全部 Patch 失败：${error.message}`, "error");
    }
  });
  els.closeDetailModalBtn.addEventListener("click", closeModal);
  document.querySelector("[data-close='modal']").addEventListener("click", closeModal);
  els.closeCopyFallbackBtn.addEventListener("click", closeCopyFallback);
  document.querySelector("[data-close='copy-fallback']").addEventListener("click", closeCopyFallback);
  els.fileSearchInput.addEventListener("input", () => {
    state.fileSearch = els.fileSearchInput.value;
    renderFileList(state.sample || { evidence_files: [] });
    if (state.sample && state.selectedFilePath) {
      selectFile(state.selectedFilePath);
    }
  });
  els.fileFilterSelect.addEventListener("change", () => {
    state.fileFilter = els.fileFilterSelect.value;
    renderFileList(state.sample || { evidence_files: [] });
    if (state.sample && state.selectedFilePath) {
      selectFile(state.selectedFilePath);
    }
  });

  setupAccordion();
  setupSplitter();
  await loadSamples();
  startAutoRefresh();
}

init().catch((error) => {
  setSaveStatus(`初始化失败：${error.message}`, "error");
});
