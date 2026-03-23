// =========================
// API CONFIG
// =========================
const API_URL = "https://projet-mastercard-g2a-aurat-hackathon-ia.onrender.com";
const KPI_LABELS = {
    frequentation: "Frequentation"
};

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
const pointFilters = Array.from(document.querySelectorAll(".point-filter"));
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
let frequentationByDepartment = {};
let currentRange = { min: 0, max: 100 };
let stationsData = null;
let stationsLayer = null;

const STATION_TYPE_STYLES = {
    "Ski Alpin": { color: AURA_COLORS.brique },
    "Ski Nordique": { color: AURA_COLORS.sapin },
    "Ski Alpin et Ski Nordique": { color: AURA_COLORS.bronze }
};

if (window.proj4) {
    proj4.defs(
        "EPSG:2154",
        "+proj=lcc +lat_1=49 +lat_2=44 +lat_0=46.5 +lon_0=3 +x_0=700000 +y_0=6600000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs"
    );
}

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
    const frequentation = frequentationByDepartment[depName];

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
            Frequentation: ${formatNumber(frequentation)}
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

function normalizeStationType(rawType) {
    const value = (rawType || "").toLowerCase();
    if (value.includes("alpin") && value.includes("nordique")) {
        return "Ski Alpin et Ski Nordique";
    }
    if (value.includes("nordique")) {
        return "Ski Nordique";
    }
    return "Ski Alpin";
}

function toLatLngFromLambert93(coordinates) {
    if (!window.proj4 || !Array.isArray(coordinates) || coordinates.length < 2) {
        return null;
    }
    const [lon, lat] = proj4("EPSG:2154", "EPSG:4326", coordinates);
    return [lat, lon];
}

function renderStations() {
    if (!stationsData || !stationsLayer) {
        return;
    }

    const selectedTypes = pointFilters.filter(item => item.checked).map(item => item.value);
    stationsLayer.clearLayers();

    if (selectedTypes.length === 0) {
        return;
    }

    stationsData.features.forEach(feature => {
        const stationType = normalizeStationType(feature.properties.TYPE);
        if (!selectedTypes.includes(stationType)) {
            return;
        }

        const latLng = toLatLngFromLambert93(feature.geometry.coordinates);
        if (!latLng) {
            return;
        }

        const styleConfig = STATION_TYPE_STYLES[stationType] || STATION_TYPE_STYLES["Ski Alpin"];
        L.circleMarker(latLng, {
            radius: 5,
            color: AURA_COLORS.bleuMarine,
            weight: 1,
            fillColor: styleConfig.color,
            fillOpacity: 0.9,
        })
            .bindPopup(
                `<strong>${feature.properties.NOMSTATION}</strong><br>${stationType}<br>${feature.properties.REMARQUES || ""}`
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
            renderStations();
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
    areaFilters.forEach(input => {
        input.addEventListener("change", event => {
            const selected = event.target.value;
            activeKpi = selected === "none" ? null : selected;
            fetchKpiData();
        });
    });

    pointFilters.forEach(input => {
        input.addEventListener("change", () => {
            renderStations();
            appendDebugLine("points filters", {
                selected: pointFilters.filter(item => item.checked).map(item => item.value)
            });
        });
    });
}

Promise.all([
    fetch("data/departements.geojson").then(res => res.json()),
    fetch("data/stations.geojson").then(res => res.json()),
    fetch(`${API_URL}/api/hello`).then(res => res.json())
])
    .then(([geojson, stationsGeojson]) => {
        geoData = geojson;
        stationsData = stationsGeojson;
        stationsLayer = L.layerGroup().addTo(map);
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