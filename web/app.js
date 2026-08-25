const wsStatus = document.getElementById("ws-status");
const taskTableBody = document.getElementById("task-table-body");
const taskForm = document.getElementById("task-form");

const mSubmitted = document.getElementById("m-submitted");
const mCompleted = document.getElementById("m-completed");
const mFailed = document.getElementById("m-failed");

let tasks = new Map();

// Connect WebSocket
function connectWS() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${protocol}//${window.location.host}/ws`);

  ws.onopen = () => {
    wsStatus.textContent = "Live Connected";
    wsStatus.classList.add("connected");
  };

  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.event && msg.data) {
        updateTaskInMemory(msg.data);
        renderTasks();
      }
    } catch (err) {
      console.error("Failed to parse WS message:", err);
    }
  };

  ws.onclose = () => {
    wsStatus.textContent = "Disconnected - Reconnecting...";
    wsStatus.classList.remove("connected");
    setTimeout(connectWS, 2000);
  };
}

// Fetch initial task state
async function loadTasks() {
  try {
    const res = await fetch("/tasks");
    const data = await res.json();
    if (Array.isArray(data)) {
      data.forEach(t => tasks.set(t.id, t));
      renderTasks();
    }
  } catch (err) {
    console.error("Failed to load initial tasks:", err);
  }
}

function updateTaskInMemory(task) {
  tasks.set(task.id, task);
}

function renderTasks() {
  const taskList = Array.from(tasks.values()).sort((a, b) => 
    new Date(b.created_at) - new Date(a.created_at)
  );

  let completedCount = 0;
  let failedCount = 0;

  if (taskList.length === 0) {
    taskTableBody.innerHTML = `<tr><td colspan="5" style="text-align:center; color:#64748b;">No tasks submitted yet.</td></tr>`;
    return;
  }

  taskTableBody.innerHTML = taskList.map(t => {
    if (t.status === "COMPLETED") completedCount++;
    if (t.status === "FAILED") failedCount++;

    return `
      <tr>
        <td style="font-family: monospace; color: #38bdf8;">${t.id.slice(0, 16)}...</td>
        <td>${t.name || "unnamed"}</td>
        <td><span class="state state-${t.status}">${t.status}</span></td>
        <td>${t.current_retry} / ${t.max_retries}</td>
        <td style="color: #94a3b8;">${new Date(t.created_at).toLocaleTimeString()}</td>
      </tr>
    `;
  }).join("");

  mSubmitted.textContent = taskList.length;
  mCompleted.textContent = completedCount;
  mFailed.textContent = failedCount;
}

// Handle Form Submission
taskForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  const name = document.getElementById("task-name").value;
  const payload = document.getElementById("task-payload").value;
  const priority = parseInt(document.getElementById("task-priority").value, 10);

  try {
    const res = await fetch("/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, payload, priority })
    });

    if (res.ok) {
      document.getElementById("task-name").value = "";
      document.getElementById("task-payload").value = "";
      loadTasks();
    }
  } catch (err) {
    console.error("Error submitting task:", err);
  }
});

loadTasks();
connectWS();
