// =========================
// API CONFIG
// =========================
const API_URL = "https://projet-mastercard-g2a-aurat-hackathon-ia.onrender.com";
const KPI_LABELS = {
    frequentation: "Frequentation",
    meteo: "Meteo",
    securite: "Securite"
};

const kpiFilters = Array.from(document.querySelectorAll(".kpi-filter"));
const apiStatus = document.getElementById("apiStatus");
const vizContainer = document.getElementById("viz");
const legendContainer = document.getElementById("legend");
const dataToggle = document.getElementById("dataToggle");
const dataDrawer = document.getElementById("dataDrawer");
const debugToggle = document.getElementById("debugToggle");
const debugConsole = document.getElementById("debugConsole");
const mapResizeHandle = document.getElementById("mapResizeHandle");
const layout = document.querySelector(".layout");
const mapPanel = document.querySelector(".map-panel");
const vizPanel = document.querySelector(".viz-panel");

let activeKpi = "frequentation";
let geojsonLayer;
let geoData;
let scoreByDepartment = {};
let kpisByDepartment = {};
let currentRange = { min: 0, max: 100 };

function sourceLabel(source) {
    if (source === "snowflake") return "Snowflake";
    if (source === "cache") return "Cache backend";
    if (source === "mock") return "Donnees mock";
    return "Inconnue";
}

function appendDebugLine(message, details) {
    const timestamp = new Date().toLocaleTimeString();
    const detailText = details ? ` ${JSON.stringify(details)}` : "";
    const line = `[${timestamp}] ${message}${detailText}\n`;
    debugConsole.textContent += line;
    debugConsole.scrollTop = debugConsole.scrollHeight;
}

function initUiToggles() {
    dataToggle.addEventListener("click", () => {
        dataDrawer.classList.toggle("hidden");
    });

    debugToggle.addEventListener("click", () => {
        debugConsole.classList.toggle("hidden");
    });
}

function initPanelResize() {
    if (!mapResizeHandle || !layout || !mapPanel || !vizPanel) {
        return;
    }

    mapResizeHandle.addEventListener("pointerdown", event => {
        if (window.innerWidth <= 980) {
            return;
        }

        event.preventDefault();
        document.body.classList.add("resizing");
        mapResizeHandle.setPointerCapture(event.pointerId);

        const onMove = moveEvent => {
            const rect = layout.getBoundingClientRect();
            const relativeX = moveEvent.clientX - rect.left;
            const minPercent = 35;
            const maxPercent = 75;
            const nextPercent = Math.max(minPercent, Math.min(maxPercent, (relativeX / rect.width) * 100));

            mapPanel.style.flexBasis = `${nextPercent}%`;
            vizPanel.style.flexBasis = `${100 - nextPercent}%`;
            map.invalidateSize();
        };

        const onUp = () => {
            document.body.classList.remove("resizing");
            window.removeEventListener("pointermove", onMove);
            window.removeEventListener("pointerup", onUp);
            map.invalidateSize();
        };

        window.addEventListener("pointermove", onMove);
        window.addEventListener("pointerup", onUp);
    });
}

// =========================
// MAP INIT
// =========================
const map = L.map('map').setView([45.5, 4.5], 7);

// Base map
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap'
}).addTo(map);

// Style des départements
function style(feature) {
    const depName = feature.properties.nom;
    const value = scoreByDepartment[depName];
    return {
        fillColor: getColor(value, currentRange.min, currentRange.max),
        weight: 1,
        color: "#ffffff",
        fillOpacity: 0.75
    };
}

function getColor(value, min, max) {
    if (typeof value !== "number") {
        return "#d9d9d9";
    }
    if (max === min) {
        return "#225ea8";
    }
    const ratio = Math.max(0, Math.min(1, (value - min) / (max - min)));
    if (ratio < 0.2) return "#eff3ff";
    if (ratio < 0.4) return "#bdd7e7";
    if (ratio < 0.6) return "#6baed6";
    if (ratio < 0.8) return "#3182bd";
    return "#08519c";
}

// Hover interaction
function highlightFeature(e) {
    const layer = e.target;
    layer.setStyle({
        fillOpacity: 0.95,
        weight: 2
    });
}

// Reset hover
function resetHighlight(e) {
    geojsonLayer.resetStyle(e.target);
}

// Click interaction
function onEachFeature(feature, layer) {
    const depName = feature.properties.nom;
    const value = scoreByDepartment[depName];
    const kpis = kpisByDepartment[depName];

    layer.on({
        mouseover: highlightFeature,
        mouseout: resetHighlight,
        click: () => {
            window.location.href = `department.html?dep=${depName}`;
        }
    });

    layer.bindTooltip(
        `
            <strong>${depName}</strong><br>
            Frequentation: ${kpis?.frequentation ?? "N/A"}<br>
            Meteo: ${kpis?.meteo ?? "N/A"}<br>
            Securite: ${kpis?.securite ?? "N/A"}
        `,
        {
            sticky: true,
            direction: "top",
            className: "dep-tooltip"
        }
    );

    layer.bindPopup(`${depName}<br>${KPI_LABELS[activeKpi]}: ${value ?? "N/A"}`);
}

function renderLegend() {
    legendContainer.innerHTML = `
        <strong>${KPI_LABELS[activeKpi]}</strong>
        <div class="legend-scale">
            <span class="scale-box c1"></span>
            <span class="scale-box c2"></span>
            <span class="scale-box c3"></span>
            <span class="scale-box c4"></span>
            <span class="scale-box c5"></span>
        </div>
        <div class="legend-range">${currentRange.min} - ${currentRange.max}</div>
    `;
}

function renderSummary(departments, ranges) {
    const topDepartments = [...departments]
        .sort((a, b) => b.score - a.score)
        .slice(0, 5);

    vizContainer.innerHTML = `
        <p><strong>KPI actif:</strong> ${KPI_LABELS[activeKpi]}</p>
        <p><strong>Moyenne regionale:</strong> ${ranges.avg}</p>
        <div class="bars">
            ${topDepartments
                .map(
                    item => `
                        <div class="bar-row">
                            <span>${item.name}</span>
                            <div class="bar-track">
                                <div class="bar-fill" style="width:${item.score}%;"></div>
                            </div>
                            <strong>${item.score}</strong>
                        </div>
                    `
                )
                .join("")}
        </div>
    `;
}

function updateScoreMapping(departments) {
    scoreByDepartment = {};
    kpisByDepartment = {};
    departments.forEach(dep => {
        scoreByDepartment[dep.name] = dep.score;
        kpisByDepartment[dep.name] = {
            frequentation: dep.frequentation,
            meteo: dep.meteo,
            securite: dep.securite
        };
    });
}

function renderGeoJson() {
    if (!geoData) {
        return;
    }
    if (geojsonLayer) {
        geojsonLayer.remove();
    }
    geojsonLayer = L.geoJSON(geoData, {
        style,
        onEachFeature
    }).addTo(map);
}

function fetchKpiData() {
    apiStatus.textContent = `Chargement du KPI ${KPI_LABELS[activeKpi]}...`;
    fetch(`${API_URL}/api/data?kpi=${activeKpi}`)
        .then(res => {
            if (!res.ok) {
                throw new Error("API non disponible");
            }
            return res.json();
        })
        .then(payload => {
            updateScoreMapping(payload.departments);
            currentRange = payload.ranges;
            renderLegend();
            renderSummary(payload.departments, payload.ranges);
            renderGeoJson();
            apiStatus.textContent = `Donnees synchronisees (${sourceLabel(payload.data_source)})`;
            if (payload.data_source === "mock" && payload.snowflake_error) {
                apiStatus.textContent += ` - fallback: ${payload.snowflake_error}`;
            }
            console.log("[API /api/data] data_source:", payload.data_source, "cache:", payload.cache);
            appendDebugLine("/api/data", { source: payload.data_source, cache: payload.cache });
            if (payload.snowflake_error) {
                console.log("[API /api/data] snowflake_error:", payload.snowflake_error);
                appendDebugLine("snowflake_error", { message: payload.snowflake_error });
            }
            checkSnowflakeStatus();
        })
        .catch(() => {
            apiStatus.textContent = "Erreur de chargement API";
            vizContainer.innerHTML = "Impossible de recuperer les donnees KPI.";
            appendDebugLine("/api/data error");
        });
}

function checkSnowflakeStatus() {
    fetch(`${API_URL}/api/snowflake/status`)
        .then(res => {
            if (!res.ok) {
                throw new Error("Status endpoint unavailable");
            }
            return res.json();
        })
        .then(status => {
            if (status.configured) {
                apiStatus.textContent = "Backend configure pour Snowflake (verification des donnees en cours...)";
            } else {
                apiStatus.textContent = "Snowflake non configure: fallback mock actif";
            }
            console.log("[API /api/snowflake/status]", status);
            appendDebugLine("/api/snowflake/status", status);
        })
        .catch(() => {
            console.log("[API /api/snowflake/status] indisponible");
            appendDebugLine("/api/snowflake/status indisponible");
        });
}

function initFilters() {
    kpiFilters.forEach(input => {
        input.addEventListener("change", event => {
            if (!event.target.checked) {
                const checkedCount = kpiFilters.filter(item => item.checked).length;
                if (checkedCount === 0) {
                    event.target.checked = true;
                    return;
                }
            }

            kpiFilters.forEach(item => {
                if (item !== event.target) {
                    item.checked = false;
                }
            });

            activeKpi = event.target.value;
            fetchKpiData();
        });
    });
}

Promise.all([
    fetch("data/departements.geojson").then(res => res.json()),
    fetch(`${API_URL}/api/hello`).then(res => res.json())
])
    .then(([geojson]) => {
        geoData = geojson;
        initUiToggles();
        initPanelResize();
        checkSnowflakeStatus();
        initFilters();
        fetchKpiData();
    })
    .catch(() => {
        apiStatus.textContent = "Backend indisponible";
        appendDebugLine("Backend indisponible");
    });