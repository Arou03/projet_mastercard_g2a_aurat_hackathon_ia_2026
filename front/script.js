// =========================
// API CONFIG
// =========================
const API_URL = "https://projet-mastercard-g2a-aurat-hackathon-ia.onrender.com";
const FETCH_TIMEOUT_MS = 15000;
const KPI_LABELS = {
    total_aura: "Total AURA",
    rural: "Rural",
    urbain: "Urbain",
    stations_montagne: "Stations montagne",
    villages_montagne: "Villages montagne"
};

// THEME MANAGEMENT
function initThemeToggle() {
    const themeToggle = document.getElementById("themeToggle");
    if (!themeToggle) {
        return;
    }

    const savedTheme = localStorage.getItem("appTheme") || "dark";
    
    // Apply saved theme on load
    if (savedTheme === "light") {
        document.body.classList.add("light-mode");
        themeToggle.textContent = "☀️";
    } else {
        document.body.classList.remove("light-mode");
        themeToggle.textContent = "🌙";
    }
    
    // Toggle on click
    themeToggle.addEventListener("click", () => {
        document.body.classList.toggle("light-mode");
        const isLight = document.body.classList.contains("light-mode");
        localStorage.setItem("appTheme", isLight ? "light" : "dark");
        themeToggle.textContent = isLight ? "☀️" : "🌙";
    });
}

const AURA_COLORS = {
    bleuCiel: "#00a0df",
    bleuMarine: "#162c4a",
    bleuWeb: "#086cb2",
    brique: "#d14247",
    sapin: "#1a7251",
    lila: "#90437d",
    bronze: "#d4a434",
    grad1: "#d9f1fb",
    grad2: "#9fdcf2",
    grad3: "#66c7ea",
};

const areaFilters = Array.from(document.querySelectorAll(".area-filter"));
const apiStatus = document.getElementById("apiStatus");
const vizContainer = document.getElementById("viz");
const legendContainer = document.getElementById("legend");
const dataToggle = document.getElementById("dataToggle");
const dataDrawer = document.getElementById("dataDrawer");
const weekFiltersContainer = document.getElementById("weekFilters");
const stationActivityFiltersContainer = document.getElementById("stationActivityFilters");
const debugToggle = document.getElementById("debugToggle");
const debugConsole = document.getElementById("debugConsole");
const mapResizeHandle = document.getElementById("mapResizeHandle");
const layout = document.querySelector(".layout");
const mapPanel = document.querySelector(".map-panel");
const vizPanel = document.querySelector(".viz-panel");

let activeKpi = "total_aura";
let geojsonLayer;
let geoData;
let scoreByDepartment = {};
let frequentationByDepartment = {};
let currentRange = { min: 0, max: 100 };
let availableWeeks = [];
let selectedWeeks = [];
let stationPoints = [];
let availableStationActivities = [];
let selectedStationActivities = [];
let stationsLayer = null;

function sourceLabel(source) {
    if (source === "snowflake") return "Snowflake";
    if (source === "cache") return "Cache backend";
    if (source === "mock") return "Donnees mock";
    return "Inconnue";
}

function formatNumber(value) {
    if (typeof value !== "number" || Number.isNaN(value)) {
        return "N/A";
    }
    return new Intl.NumberFormat("fr-FR").format(value);
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

function weekSortKey(week) {
    const text = String(week || "").toUpperCase();
    if (text.startsWith("S") && /^\d+$/.test(text.slice(1))) {
        return Number.parseInt(text.slice(1), 10);
    }
    return 999;
}

function renderWeekFilters() {
    if (!weekFiltersContainer) {
        return;
    }

    if (!availableWeeks.length) {
        weekFiltersContainer.textContent = "Aucune semaine disponible";
        return;
    }

    const selectedSet = new Set(selectedWeeks);
    weekFiltersContainer.innerHTML = availableWeeks
        .map(week => {
            const checked = selectedSet.has(week) ? "checked" : "";
            return `<label><input type="checkbox" class="week-filter" value="${week}" ${checked}> ${week}</label>`;
        })
        .join("");

    const weekInputs = Array.from(document.querySelectorAll(".week-filter"));
    weekInputs.forEach(input => {
        input.addEventListener("change", () => {
            selectedWeeks = weekInputs.filter(item => item.checked).map(item => item.value);
            appendDebugLine("weeks filters", { selected: selectedWeeks });
            fetchKpiData();
        });
    });
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
    const showFill = Boolean(activeKpi);
    return {
        fillColor: showFill ? getColor(value, currentRange.min, currentRange.max) : "transparent",
        weight: 1,
        color: "#ffffff",
        fillOpacity: showFill ? 0.75 : 0
    };
}

function getColor(value, min, max) {
    if (typeof value !== "number") {
        return "#d9d9d9";
    }
    if (max === min) {
        return AURA_COLORS.bleuWeb;
    }
    const ratio = Math.max(0, Math.min(1, (value - min) / (max - min)));
    if (ratio < 0.2) return AURA_COLORS.grad1;
    if (ratio < 0.4) return AURA_COLORS.grad2;
    if (ratio < 0.6) return AURA_COLORS.grad3;
    if (ratio < 0.8) return AURA_COLORS.bleuCiel;
    return AURA_COLORS.bleuWeb;
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
    const totalAura = frequentationByDepartment[depName];
    const metricLabel = activeKpi ? KPI_LABELS[activeKpi] : "Total AURA";
    const metricValue = activeKpi ? value : totalAura;

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
            ${metricLabel}: ${formatNumber(metricValue)}
        `,
        {
            sticky: true,
            direction: "top",
            className: "dep-tooltip"
        }
    );

    if (activeKpi) {
        layer.bindPopup(`${depName}<br>${KPI_LABELS[activeKpi]}: ${formatNumber(value)}`);
    } else {
        layer.bindPopup(depName);
    }
}

function renderLegend() {
    if (!activeKpi) {
        legendContainer.innerHTML = `<strong>Aucun remplissage actif</strong><div class="legend-range">Carte departementale uniquement</div>`;
        return;
    }

    legendContainer.innerHTML = `
        <strong>${KPI_LABELS[activeKpi]}</strong>
        <div class="legend-scale">
            <span class="scale-box c1"></span>
            <span class="scale-box c2"></span>
            <span class="scale-box c3"></span>
            <span class="scale-box c4"></span>
            <span class="scale-box c5"></span>
        </div>
        <div class="legend-range">${formatNumber(currentRange.min)} - ${formatNumber(currentRange.max)}</div>
    `;
}

function renderSummary(departments, ranges) {
    if (!activeKpi) {
        vizContainer.innerHTML = "Selectionne un indicateur de zone ou affiche des points stations.";
        return;
    }

    const topDepartments = [...departments]
        .sort((a, b) => b.score - a.score)
        .slice(0, 5);

    const maxScore = Math.max(...topDepartments.map(item => item.score), 0);

    vizContainer.innerHTML = `
        <p><strong>KPI actif:</strong> ${KPI_LABELS[activeKpi]}</p>
        <p><strong>Moyenne regionale:</strong> ${formatNumber(ranges.avg)}</p>
        <div class="bars">
            ${topDepartments
                .map(
                    item => `
                        <div class="bar-row">
                            <span>${item.name}</span>
                            <div class="bar-track">
                                <div class="bar-fill" style="width:${maxScore > 0 ? (item.score / maxScore) * 100 : 0}%;"></div>
                            </div>
                            <strong>${formatNumber(item.score)}</strong>
                        </div>
                    `
                )
                .join("")}
        </div>
    `;
}

function updateScoreMapping(departments) {
    scoreByDepartment = {};
    frequentationByDepartment = {};
    departments.forEach(dep => {
        scoreByDepartment[dep.name] = dep.score;
        frequentationByDepartment[dep.name] = dep.frequentation;
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

function colorForActivity(activity) {
    const palette = [
        AURA_COLORS.brique,
        AURA_COLORS.sapin,
        AURA_COLORS.bronze,
        AURA_COLORS.lila,
        AURA_COLORS.bleuCiel,
    ];
    const value = String(activity || "").toLowerCase();
    let hash = 0;
    for (let i = 0; i < value.length; i += 1) {
        hash = ((hash << 5) - hash) + value.charCodeAt(i);
        hash |= 0;
    }
    const index = Math.abs(hash) % palette.length;
    return palette[index];
}

function renderStationActivityFilters() {
    if (!stationActivityFiltersContainer) {
        return;
    }

    if (!availableStationActivities.length) {
        stationActivityFiltersContainer.textContent = "Aucune activite disponible";
        return;
    }

    const selectedSet = new Set(selectedStationActivities);
    stationActivityFiltersContainer.innerHTML = availableStationActivities
        .map(activity => {
            const checked = selectedSet.has(activity) ? "checked" : "";
            return `<label><input type="checkbox" class="point-filter" value="${activity}" ${checked}> ${activity}</label>`;
        })
        .join("");

    const pointFilters = Array.from(document.querySelectorAll(".point-filter"));
    pointFilters.forEach(input => {
        input.addEventListener("change", () => {
            selectedStationActivities = pointFilters.filter(item => item.checked).map(item => item.value);
            renderStations();
            appendDebugLine("points filters", { selected: selectedStationActivities });
        });
    });
}

function fetchStations() {
    const query = new URLSearchParams();
    if (selectedStationActivities.length > 0) {
        query.set("activities", selectedStationActivities.join(","));
    }

    const suffix = query.toString() ? `?${query.toString()}` : "";
    apiStatus.textContent = "Chargement des points d'intérêt...";
    
    return fetchWithTimeout(`${API_URL}/api/stations${suffix}`)
        .then(res => {
            if (!res.ok) {
                throw new Error(`API stations failed: ${res.status}`);
            }
            return res.json();
        })
        .then(payload => {
            stationPoints = payload.points || [];
            availableStationActivities = payload.activities_available || [];
            
            // Si aucune activité sélectionnée, sélectionner toutes les disponibles
            if (selectedStationActivities.length === 0 && availableStationActivities.length > 0) {
                selectedStationActivities = [...availableStationActivities];
            }
            
            renderStationActivityFilters();
            renderStations();
            
            appendDebugLine("/api/stations", {
                source: payload.data_source,
                count: stationPoints.length,
                activities_available: availableStationActivities.length,
                activities_selected: selectedStationActivities.length,
            });
            
            apiStatus.textContent = `Points d'intérêt chargés (${stationPoints.length} points, ${availableStationActivities.length} activités)`;
        })
        .catch(error => {
            stationPoints = [];
            availableStationActivities = [];
            selectedStationActivities = [];
            renderStationActivityFilters();
            renderStations();
            appendDebugLine("/api/stations error", { message: error?.message || "unknown" });
            apiStatus.textContent = "Erreur chargement points d'intérêt";
        });
}

function renderStations() {
    if (!stationsLayer) {
        return;
    }

    const selectedActivitiesSet = new Set(selectedStationActivities);
    stationsLayer.clearLayers();

    if (selectedActivitiesSet.size === 0) {
        return;
    }

    stationPoints.forEach(point => {
        const pointActivities = Array.isArray(point.activities) ? point.activities : [];
        const match = pointActivities.some(activity => selectedActivitiesSet.has(activity));
        if (!match) {
            return;
        }

        if (typeof point.lat !== "number" || typeof point.lon !== "number") {
            return;
        }

        const firstActivity = pointActivities[0] || "Activite non renseignee";
        L.circleMarker([point.lat, point.lon], {
            radius: 5,
            color: AURA_COLORS.bleuMarine,
            weight: 1,
            fillColor: colorForActivity(firstActivity),
            fillOpacity: 0.9,
        })
            .bindPopup(
                `<strong>${point.name || "Installation sans nom"}</strong><br>${point.equipment_type || "Type inconnu"}<br>${point.department_name || ""}<br>${pointActivities.join(", ") || "Aucune activite"}`
            )
            .addTo(stationsLayer);
    });
}

function fetchKpiData() {
    if (!activeKpi) {
        scoreByDepartment = {};
        currentRange = { min: 0, max: 100 };
        renderLegend();
        renderSummary([], { avg: "-" });
        renderGeoJson();
        renderStations();
        apiStatus.textContent = "Carte departementale sans remplissage (mode vierge)";
        appendDebugLine("mode area", { selected: "none" });
        return;
    }

    apiStatus.textContent = `Chargement du KPI ${KPI_LABELS[activeKpi]}...`;

    const query = new URLSearchParams({ kpi: activeKpi });
    if (selectedWeeks.length > 0) {
        query.set("weeks", selectedWeeks.join(","));
    }

    fetchWithTimeout(`${API_URL}/api/data?${query.toString()}`)
        .then(res => {
            if (!res.ok) {
                throw new Error("API non disponible");
            }
            return res.json();
        })
        .then(payload => {
            availableWeeks = [...(payload.weeks_available || [])].sort((a, b) => weekSortKey(a) - weekSortKey(b));
            selectedWeeks = payload.weeks_selected || selectedWeeks;
            renderWeekFilters();

            updateScoreMapping(payload.departments);
            currentRange = payload.ranges;
            renderLegend();
            renderSummary(payload.departments, payload.ranges);
            renderGeoJson();
            renderStations();
            apiStatus.textContent = `Donnees synchronisees (${sourceLabel(payload.data_source)})`;
            if (selectedWeeks.length > 0) {
                apiStatus.textContent += ` - Semaines: ${selectedWeeks.join(", ")}`;
            }
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
        .catch(error => {
            if (error && error.name === "AbortError") {
                apiStatus.textContent = "Timeout API: reponse trop lente";
            } else {
                apiStatus.textContent = "Erreur de chargement API";
            }
            vizContainer.innerHTML = "Impossible de recuperer les donnees KPI.";
            appendDebugLine("/api/data error", { message: error?.message || "unknown" });
        });
}

function checkSnowflakeStatus() {
    fetchWithTimeout(`${API_URL}/api/snowflake/status`, 8000)
        .then(res => {
            if (!res.ok) {
                throw new Error("Status endpoint unavailable");
            }
            return res.json();
        })
        .then(status => {
            console.log("[API /api/snowflake/status]", status);
            appendDebugLine("/api/snowflake/status", status);
        })
        .catch(() => {
            console.log("[API /api/snowflake/status] indisponible");
            appendDebugLine("/api/snowflake/status indisponible");
        });
}

function initFilters() {
    areaFilters.forEach(input => {
        input.addEventListener("change", event => {
            const selected = event.target.value;
            activeKpi = selected === "none" ? null : selected;
            fetchKpiData();
        });
    });
}

fetch("data/departements.geojson")
    .then(res => res.json())
    .then(geojson => {
        geoData = geojson;
        stationsLayer = L.layerGroup().addTo(map);
        initThemeToggle();
        initUiToggles();
        initPanelResize();
        checkSnowflakeStatus();
        initFilters();
        fetchKpiData();
        fetchStations();
    })
    .catch(() => {
        apiStatus.textContent = "Backend indisponible";
        appendDebugLine("Backend indisponible");
    });