const API_URL = "https://projet-mastercard-g2a-aurat-hackathon-ia.onrender.com";
const FETCH_TIMEOUT_MS = 15000;

const resultsContainer = document.getElementById("resultsContainer");
const statusBadge = document.getElementById("statusBadge");
const testBtn = document.getElementById("testBtn");
const clearBtn = document.getElementById("clearBtn");
const backBtn = document.getElementById("backBtn");

function initThemeToggle() {
    const themeToggle = document.getElementById("themeToggle");
    if (!themeToggle) {
        return;
    }

    const savedTheme = localStorage.getItem("appTheme") || "dark";
    if (savedTheme === "light") {
        document.body.classList.add("light-mode");
        themeToggle.textContent = "☀️";
    } else {
        document.body.classList.remove("light-mode");
        themeToggle.textContent = "🌙";
    }

    themeToggle.addEventListener("click", () => {
        document.body.classList.toggle("light-mode");
        const isLight = document.body.classList.contains("light-mode");
        localStorage.setItem("appTheme", isLight ? "light" : "dark");
        themeToggle.textContent = isLight ? "☀️" : "🌙";
    });
}

function showStatus(message, type) {
    statusBadge.className = `status-badge status-${type}`;
    statusBadge.textContent = message;
    statusBadge.style.display = "block";
}

function createResultSection(title, content) {
    const section = document.createElement("div");
    section.className = "result-section active";
    section.innerHTML = `<h3>${title}</h3>${content}`;
    return section;
}

function escapeHtml(text) {
    const map = {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#039;",
    };
    return String(text).replace(/[&<>"']/g, m => map[m]);
}

function formatJson(obj, indent = 0) {
    if (obj === null) return '<span class="json-boolean">null</span>';
    if (typeof obj === "boolean") return `<span class="json-boolean">${obj}</span>`;
    if (typeof obj === "number") return `<span class="json-number">${obj}</span>`;
    if (typeof obj === "string") return `<span class="json-string">"${escapeHtml(obj)}"</span>`;

    const spaces = "  ".repeat(indent);
    const nextSpaces = "  ".repeat(indent + 1);

    if (Array.isArray(obj)) {
        if (obj.length === 0) return "[]";
        const items = obj.map(item => `${nextSpaces}${formatJson(item, indent + 1)}`).join(",\n");
        return `[\n${items}\n${spaces}]`;
    }

    if (typeof obj === "object") {
        const keys = Object.keys(obj);
        if (keys.length === 0) return "{}";
        const items = keys
            .map(key => `${nextSpaces}<span class="json-key">"${key}"</span>: ${formatJson(obj[key], indent + 1)}`)
            .join(",\n");
        return `{\n${items}\n${spaces}}`;
    }

    return String(obj);
}

async function fetchWithTimeout(url, timeoutMs = FETCH_TIMEOUT_MS) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    try {
        return await fetch(url, { signal: controller.signal });
    } finally {
        clearTimeout(timeoutId);
    }
}

async function runTest() {
    testBtn.disabled = true;
    resultsContainer.innerHTML = "";
    showStatus("Test en cours...", "loading");

    try {
        const response = await fetchWithTimeout(`${API_URL}/api/snowflake/test/freq-globale`);
        const data = await response.json();

        if (data.success) {
            showStatus("Connexion reussie", "success");
            displaySuccessResults(data);
        } else {
            showStatus("Erreur de connexion", "error");
            displayErrorResults(data);
        }
    } catch (error) {
        const isTimeout = error && error.name === "AbortError";
        showStatus(isTimeout ? "Timeout API" : "Erreur reseau", "error");
        const errorSection = createResultSection(
            "Erreur reseau",
            `<div class="error-message">
                <strong>Impossible de contacter l'API:</strong><br>
                ${escapeHtml(isTimeout ? "Le serveur a mis trop de temps a repondre." : error.message)}
                <br><br>
                <em>Assurez-vous que le backend Render est en cours d'execution et que l'URL est correcte.</em>
            </div>`
        );
        resultsContainer.appendChild(errorSection);
    } finally {
        testBtn.disabled = false;
    }
}

function displaySuccessResults(data) {
    const connDetails = `
        <div class="success-message">
            Connexion Snowflake etablie avec succes.
        </div>
        <div class="info-card">
            <strong>Compte:</strong> ${escapeHtml(data.connection_details.account)}<br>
            <strong>Utilisateur:</strong> ${escapeHtml(data.connection_details.user)}<br>
            <strong>Base de Donnees:</strong> ${escapeHtml(data.connection_details.database)}<br>
            <strong>Schema:</strong> ${escapeHtml(data.connection_details.schema)}<br>
            <strong>Lignes trouvees:</strong> ${data.freq_globale_row_count}
        </div>
    `;
    resultsContainer.appendChild(createResultSection("Details de Connexion", connDetails));

    if (data.available_tables && data.available_tables.length > 0) {
        const tablesList = `<ul>${data.available_tables.map(t => `<li>${escapeHtml(t)}</li>`).join("")}</ul>`;
        resultsContainer.appendChild(createResultSection("Tables Disponibles", tablesList));
    }

    if (data.freq_globale_columns && data.freq_globale_columns.length > 0) {
        let columnsHtml = '<div class="info-card"><strong>Colonnes detectees:</strong><br>';
        columnsHtml += data.freq_globale_columns.map(col => `• <code>${escapeHtml(col)}</code>`).join("<br>");
        columnsHtml += "</div>";
        resultsContainer.appendChild(createResultSection("Structure FREQ_GLOBAL_PER_DEPT", columnsHtml));
    }

    if (data.freq_globale_sample && data.freq_globale_sample.length > 0) {
        const firstRow = data.freq_globale_sample[0];
        const headers = Object.keys(firstRow);

        let tableHtml = '<div class="table-wrapper"><table>';
        tableHtml += "<thead><tr>" + headers.map(h => `<th>${escapeHtml(h)}</th>`).join("") + "</tr></thead>";
        tableHtml += "<tbody>";

        data.freq_globale_sample.forEach(row => {
            tableHtml += "<tr>";
            headers.forEach(h => {
                const value = row[h];
                const displayValue = value === null ? "<em>NULL</em>" : escapeHtml(String(value));
                tableHtml += `<td>${displayValue}</td>`;
            });
            tableHtml += "</tr>";
        });

        tableHtml += "</tbody></table></div>";
        resultsContainer.appendChild(createResultSection("Donnees Echantillon", tableHtml));
    }

    const jsonSection = createResultSection(
        "Reponse JSON Complete",
        `<div class="json-output"><pre>${formatJson(data)}</pre></div>`
    );
    resultsContainer.appendChild(jsonSection);
}

function displayErrorResults(data) {
    const errorMessage = `
        <div class="error-message">
            <strong>Erreur Snowflake:</strong><br>
            ${escapeHtml(data.error)}<br><br>
            <strong>Type:</strong> ${escapeHtml(data.error_type || "Unknown")}
        </div>
        <div class="info-card" style="background: #fff3e0; border-left-color: #ff9800; color: #e65100;">
            <strong>Diagnostic:</strong><br>
            Verifiez les variables d'environnement:<br>
            • SNOWFLAKE_ACCOUNT<br>
            • SNOWFLAKE_USER<br>
            • SNOWFLAKE_PRIVATE_KEY_B64<br>
            • SNOWFLAKE_DATABASE<br>
            • SNOWFLAKE_SCHEMA
        </div>
    `;
    resultsContainer.appendChild(createResultSection("Erreur", errorMessage));

    if (data.traceback) {
        const tracebackHtml = `<div class="json-output"><pre>${escapeHtml(data.traceback)}</pre></div>`;
        resultsContainer.appendChild(createResultSection("Traceback Complet", tracebackHtml));
    }

    const jsonSection = createResultSection(
        "Reponse JSON Complete",
        `<div class="json-output"><pre>${formatJson(data)}</pre></div>`
    );
    resultsContainer.appendChild(jsonSection);
}

function clearResults() {
    resultsContainer.innerHTML = "";
    statusBadge.style.display = "none";
}

function goBack() {
    window.location.href = "index.html";
}

window.addEventListener("load", () => {
    initThemeToggle();

    if (testBtn) {
        testBtn.addEventListener("click", runTest);
    }
    if (clearBtn) {
        clearBtn.addEventListener("click", clearResults);
    }
    if (backBtn) {
        backBtn.addEventListener("click", goBack);
    }
});
