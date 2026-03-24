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
const selectAllActivitiesCheckbox = document.getElementById("selectAllActivities");
const globalYearSelector = document.getElementById("globalYearSelector");
const globalHolidayCanvas = document.getElementById("globalHolidayTimeline");
const globalHolidayLegend = document.getElementById("globalHolidayLegend");
const globalHolidayStatus = document.getElementById("globalHolidayStatus");
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
let weekRangeStartIndex = 0;
let weekRangeEndIndex = 0;
let currentYear = 2024;
let stationPoints = [];
let availableStationActivities = [];
let selectedStationActivities = [];
let stationsLayer = null;
let globalHolidaySegments = [];
let holidayTooltipEl = null;

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

function ensureHolidayTooltip() {
    if (holidayTooltipEl) {
        return holidayTooltipEl;
    }
    holidayTooltipEl = document.createElement("div");
    holidayTooltipEl.className = "holiday-tooltip hidden";
    document.body.appendChild(holidayTooltipEl);
    return holidayTooltipEl;
}

function hideHolidayTooltip() {
    if (!holidayTooltipEl) {
        return;
    }
    holidayTooltipEl.classList.add("hidden");
}

function formatDisplayDate(value) {
    const text = String(value || "").trim();
    if (!text) {
        return "-";
    }
    const date = new Date(text);
    if (Number.isNaN(date.getTime())) {
        return text;
    }
    return new Intl.DateTimeFormat("fr-FR").format(date);
}

function holidayColor(seed) {
    const palette = ["#d14247", "#1a7251", "#086cb2", "#90437d", "#d4a434", "#00a0df"];
    let hash = 0;
    const text = String(seed || "");
    for (let i = 0; i < text.length; i += 1) {
        hash = ((hash << 5) - hash) + text.charCodeAt(i);
        hash |= 0;
    }
    return palette[Math.abs(hash) % palette.length];
}

function toWeekNumber(weekLabel) {
    const text = String(weekLabel || "").toUpperCase().trim();
    if (text.startsWith("S") && /^\d+$/.test(text.slice(1))) {
        return Number.parseInt(text.slice(1), 10);
    }
    if (/^\d+$/.test(text)) {
        return Number.parseInt(text, 10);
    }
    return Number.NaN;
}

function toAxisRank(weekLabel) {
    const week = toWeekNumber(weekLabel);
    if (Number.isNaN(week)) {
        return Number.NaN;
    }
    if (week >= 51) {
        return week - 51;
    }
    return week + 1;
}

function getCanvasSize(canvas) {
    const cssWidth = Math.max(320, Math.floor(canvas.getBoundingClientRect().width));
    const cssHeight = Math.max(220, Math.floor(canvas.getBoundingClientRect().height));
    const ratio = window.devicePixelRatio || 1;
    canvas.width = Math.floor(cssWidth * ratio);
    canvas.height = Math.floor(cssHeight * ratio);
    const ctx = canvas.getContext("2d");
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    return { ctx, width: cssWidth, height: cssHeight };
}

function drawGlobalHolidayLanes(weeks, holidays, countries) {
    if (!globalHolidayCanvas || !globalHolidayLegend) {
        return;
    }

    const { ctx, width, height } = getCanvasSize(globalHolidayCanvas);
    const pad = { top: 18, right: 18, bottom: 28, left: 110 };
    const chartW = width - pad.left - pad.right;
    const chartH = height - pad.top - pad.bottom;

    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = "#f8fbff";
    ctx.fillRect(0, 0, width, height);

    const orderedCountries = countries && countries.length ? countries : ["France"];
    const laneHeight = Math.max(20, chartH / Math.max(1, orderedCountries.length));
    const weekRanks = weeks.map(week => toAxisRank(week));
    const minRank = weekRanks.length ? Math.min(...weekRanks) : 0;
    const maxRank = weekRanks.length ? Math.max(...weekRanks) : 1;
    const rankSpan = Math.max(1, maxRank - minRank);

    const xFromWeek = week => {
        const rank = toAxisRank(week);
        if (Number.isNaN(rank)) {
            return Number.NaN;
        }
        return pad.left + ((rank - minRank) / rankSpan) * chartW;
    };

    const yFromCountry = country => {
        const index = orderedCountries.indexOf(country);
        if (index < 0) {
            return Number.NaN;
        }
        return pad.top + index * laneHeight + 2;
    };

    ctx.strokeStyle = "#d4dfe9";
    ctx.lineWidth = 1;
    for (let i = 0; i <= orderedCountries.length; i += 1) {
        const y = pad.top + i * laneHeight;
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(width - pad.right, y);
        ctx.stroke();
    }

    globalHolidaySegments = [];

    holidays.forEach(item => {
        const countryName = item.country_name || item.country_code || "Inconnu";
        const y = yFromCountry(countryName);
        if (Number.isNaN(y)) {
            return;
        }

        let x1 = xFromWeek(item.week_start);
        let x2 = xFromWeek(item.week_end);
        if (Number.isNaN(x1) || Number.isNaN(x2)) {
            return;
        }
        if (x2 < x1) {
            const tmp = x1;
            x1 = x2;
            x2 = tmp;
        }

        const color = holidayColor(item.holiday_type || "VACANCES");
        const widthRect = Math.max(3, x2 - x1);
        const heightRect = Math.max(12, laneHeight - 6);
        ctx.fillStyle = color;
        ctx.globalAlpha = 0.35;
        ctx.fillRect(x1, y, widthRect, heightRect);
        ctx.globalAlpha = 1;

        globalHolidaySegments.push({
            x: x1,
            y,
            w: widthRect,
            h: heightRect,
            color,
            item,
            countryName,
        });
    });

    ctx.fillStyle = "#162c4a";
    ctx.font = "12px Luciole, Segoe UI, sans-serif";
    orderedCountries.forEach((country, index) => {
        const y = pad.top + index * laneHeight + laneHeight / 2 + 4;
        ctx.textAlign = "right";
        ctx.fillText(country, pad.left - 8, y);
    });

    const tickIndexes = [0, Math.floor((weeks.length - 1) / 3), Math.floor(((weeks.length - 1) * 2) / 3), Math.max(0, weeks.length - 1)];
    const seen = new Set();
    tickIndexes.forEach(index => {
        if (seen.has(index) || !weeks[index]) {
            return;
        }
        seen.add(index);
        const x = xFromWeek(weeks[index]);
        ctx.textAlign = "center";
        ctx.fillText(weeks[index], x, height - 8);
    });

    const holidayTypes = [...new Set(holidays.map(item => item.holiday_type).filter(Boolean))];
    globalHolidayLegend.innerHTML = holidayTypes.length
        ? holidayTypes
            .map(type => {
                const color = holidayColor(type);
                return `<span class="legend-chip"><span class="legend-dot" style="background:${color}"></span>${type}</span>`;
            })
            .join("")
        : "Aucune periode de vacances disponible";

    const tooltip = ensureHolidayTooltip();
    globalHolidayCanvas.onmousemove = event => {
        const rect = globalHolidayCanvas.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;
        const hit = globalHolidaySegments.find(
            segment => x >= segment.x && x <= segment.x + segment.w && y >= segment.y && y <= segment.y + segment.h
        );

        if (!hit) {
            globalHolidayCanvas.style.cursor = "default";
            hideHolidayTooltip();
            return;
        }

        globalHolidayCanvas.style.cursor = "pointer";
        const period = `${formatDisplayDate(hit.item.date_start)} -> ${formatDisplayDate(hit.item.date_end)}`;
        tooltip.innerHTML = `
            <strong>${hit.countryName}</strong><br>
            ${hit.item.holiday_type || "Type inconnu"}<br>
            ${period}
        `;
        tooltip.style.left = `${event.clientX + 14}px`;
        tooltip.style.top = `${event.clientY + 14}px`;
        tooltip.classList.remove("hidden");
    };

    globalHolidayCanvas.onmouseleave = () => {
        globalHolidayCanvas.style.cursor = "default";
        hideHolidayTooltip();
    };
}

function fetchGlobalHolidays() {
    if (!globalHolidayCanvas || !globalHolidayStatus) {
        return Promise.resolve();
    }

    globalHolidayStatus.textContent = "Chargement...";
    const query = new URLSearchParams({ year: String(currentYear) });

    return fetchWithTimeout(`${API_URL}/api/global/holidays?${query.toString()}`)
        .then(res => {
            if (!res.ok) {
                throw new Error(`API holidays failed: ${res.status}`);
            }
            return res.json();
        })
        .then(payload => {
            const weeks = normalizeWeeksOrder(payload.weeks || []);
            const holidays = payload.holidays || [];
            const countries = payload.countries || [];
            drawGlobalHolidayLanes(weeks, holidays, countries);
            globalHolidayStatus.textContent = `${sourceLabel(payload.data_source)} - ${countries.length} pays`;
            appendDebugLine("/api/global/holidays", {
                source: payload.data_source,
                countries: countries.length,
                holidays: holidays.length,
                year: currentYear,
            });
        })
        .catch(error => {
            globalHolidayStatus.textContent = "Erreur chargement vacances";
            if (globalHolidayLegend) {
                globalHolidayLegend.textContent = "Impossible de recuperer le calendrier des vacances.";
            }
            appendDebugLine("/api/global/holidays error", { message: error?.message || "unknown" });
        });
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
        const weekNumber = Number.parseInt(text.slice(1), 10);
        if (weekNumber === 51) return 0;
        if (weekNumber === 52) return 1;
        if (weekNumber >= 1 && weekNumber <= 15) return weekNumber + 1;
        return weekNumber + 200;
    }
    return 999;
}

function normalizeWeeksOrder(weeks) {
    const unique = [...new Set((weeks || []).map(item => String(item || "").toUpperCase()).filter(Boolean))];
    return unique.sort((a, b) => weekSortKey(a) - weekSortKey(b));
}

function syncWeekSelectionFromRange() {
    if (!availableWeeks.length) {
        selectedWeeks = [];
        weekRangeStartIndex = 0;
        weekRangeEndIndex = 0;
        return;
    }

    weekRangeStartIndex = Math.max(0, Math.min(weekRangeStartIndex, availableWeeks.length - 1));
    weekRangeEndIndex = Math.max(0, Math.min(weekRangeEndIndex, availableWeeks.length - 1));
    if (weekRangeStartIndex > weekRangeEndIndex) {
        const tmp = weekRangeStartIndex;
        weekRangeStartIndex = weekRangeEndIndex;
        weekRangeEndIndex = tmp;
    }

    selectedWeeks = availableWeeks.slice(weekRangeStartIndex, weekRangeEndIndex + 1);
}

function initWeekRangeFromSelection() {
    if (!availableWeeks.length) {
        weekRangeStartIndex = 0;
        weekRangeEndIndex = 0;
        selectedWeeks = [];
        return;
    }

    if (!selectedWeeks.length) {
        weekRangeStartIndex = 0;
        weekRangeEndIndex = availableWeeks.length - 1;
        syncWeekSelectionFromRange();
        return;
    }

    const selectedSet = new Set(selectedWeeks);
    const indexes = availableWeeks
        .map((week, index) => (selectedSet.has(week) ? index : -1))
        .filter(index => index !== -1);

    if (!indexes.length) {
        weekRangeStartIndex = 0;
        weekRangeEndIndex = availableWeeks.length - 1;
    } else {
        weekRangeStartIndex = Math.min(...indexes);
        weekRangeEndIndex = Math.max(...indexes);
    }
    syncWeekSelectionFromRange();
}

function renderWeekFilters() {
    if (!weekFiltersContainer) {
        return;
    }

    if (!availableWeeks.length) {
        weekFiltersContainer.textContent = "Aucune semaine disponible";
        return;
    }

    initWeekRangeFromSelection();

    weekFiltersContainer.innerHTML = `
        <div class="week-range-box">
            <div class="week-range-values">
                <span id="weekStartValue">${availableWeeks[weekRangeStartIndex]}</span>
                <span id="weekEndValue">${availableWeeks[weekRangeEndIndex]}</span>
            </div>
            <div class="week-range-single" id="weekRangeSingle">
                <div class="week-range-track"></div>
                <div class="week-range-fill" id="weekRangeFill"></div>
                <input id="weekStartRange" class="week-range-input week-range-input-start" type="range" min="0" max="${availableWeeks.length - 1}" value="${weekRangeStartIndex}">
                <input id="weekEndRange" class="week-range-input week-range-input-end" type="range" min="0" max="${availableWeeks.length - 1}" value="${weekRangeEndIndex}">
            </div>
        </div>
    `;

    const startInput = document.getElementById("weekStartRange");
    const endInput = document.getElementById("weekEndRange");
    const startValue = document.getElementById("weekStartValue");
    const endValue = document.getElementById("weekEndValue");
    const rangeFill = document.getElementById("weekRangeFill");

    const refreshRangeUI = () => {
        const maxIndex = Math.max(1, availableWeeks.length - 1);
        const startPercent = (weekRangeStartIndex / maxIndex) * 100;
        const endPercent = (weekRangeEndIndex / maxIndex) * 100;

        startValue.textContent = availableWeeks[weekRangeStartIndex];
        endValue.textContent = availableWeeks[weekRangeEndIndex];
        if (rangeFill) {
            rangeFill.style.left = `${startPercent}%`;
            rangeFill.style.width = `${Math.max(0, endPercent - startPercent)}%`;
        }
    };

    startInput.addEventListener("input", () => {
        weekRangeStartIndex = Number.parseInt(startInput.value, 10);
        if (weekRangeStartIndex > weekRangeEndIndex) {
            weekRangeEndIndex = weekRangeStartIndex;
            endInput.value = String(weekRangeEndIndex);
        }
        syncWeekSelectionFromRange();
        refreshRangeUI();
    });

    endInput.addEventListener("input", () => {
        weekRangeEndIndex = Number.parseInt(endInput.value, 10);
        if (weekRangeEndIndex < weekRangeStartIndex) {
            weekRangeStartIndex = weekRangeEndIndex;
            startInput.value = String(weekRangeStartIndex);
        }
        syncWeekSelectionFromRange();
        refreshRangeUI();
    });

    const triggerReload = () => {
        appendDebugLine("weeks range", {
            start: availableWeeks[weekRangeStartIndex],
            end: availableWeeks[weekRangeEndIndex],
            selected: selectedWeeks,
        });
        fetchKpiData();
    };

    startInput.addEventListener("change", triggerReload);
    endInput.addEventListener("change", triggerReload);
    refreshRangeUI();
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

function initGlobalYearSelector() {
    if (!globalYearSelector) {
        return;
    }

    const parsedYear = Number.parseInt(globalYearSelector.value, 10);
    currentYear = Number.isNaN(parsedYear) ? 2024 : parsedYear;

    globalYearSelector.addEventListener("change", event => {
        const selectedYear = Number.parseInt(event.target.value, 10);
        currentYear = Number.isNaN(selectedYear) ? 2024 : selectedYear;
        appendDebugLine("year changed", { year: currentYear });
        fetchKpiData();
        fetchStations();
        fetchGlobalHolidays();
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
        mouseout: resetHighlight
        // DISABLED: ML analysis will focus on global AURA data
        // click: () => {
        //     window.location.href = `department.html?dep=${depName}`;
        // }
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
    // KPI summary panel intentionally removed from homepage.
    return;
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
        if (selectAllActivitiesCheckbox) {
            selectAllActivitiesCheckbox.checked = false;
            selectAllActivitiesCheckbox.disabled = true;
        }
        return;
    }

    if (selectAllActivitiesCheckbox) {
        selectAllActivitiesCheckbox.disabled = false;
    }

    const selectedSet = new Set(selectedStationActivities);
    stationActivityFiltersContainer.innerHTML = availableStationActivities
        .map(activity => {
            const checked = selectedSet.has(activity) ? "checked" : "";
            return `<label><input type="checkbox" class="point-filter" value="${activity}" ${checked}> ${activity}</label>`;
        })
        .join("");

    const pointFilters = Array.from(document.querySelectorAll(".point-filter"));

    const syncSelectAllState = () => {
        if (!selectAllActivitiesCheckbox) {
            return;
        }
        const selectedSet = new Set(selectedStationActivities);
        const allChecked = availableStationActivities.length > 0 && availableStationActivities.every(activity => selectedSet.has(activity));
        selectAllActivitiesCheckbox.checked = allChecked;
    };

    pointFilters.forEach(input => {
        input.addEventListener("change", () => {
            selectedStationActivities = pointFilters.filter(item => item.checked).map(item => item.value);
            syncSelectAllState();
            renderStations();
            appendDebugLine("points filters", { selected: selectedStationActivities });
        });
    });

    if (selectAllActivitiesCheckbox) {
        selectAllActivitiesCheckbox.onchange = () => {
            const shouldCheck = selectAllActivitiesCheckbox.checked;
            pointFilters.forEach(input => {
                input.checked = shouldCheck;
            });
            selectedStationActivities = shouldCheck ? [...availableStationActivities] : [];
            renderStations();
            appendDebugLine("points filters all", { enabled: shouldCheck, selected: selectedStationActivities });
        };
        syncSelectAllState();
    }

}

function fetchStations() {
    const query = new URLSearchParams();
    query.set("year", String(currentYear));
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
            .bindTooltip(
                `<strong>${point.name || "Installation sans nom"}</strong><br>Categories: ${pointActivities.join(", ") || "Aucune activite"}`,
                {
                    sticky: true,
                    direction: "top",
                }
            )
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

    const query = new URLSearchParams({
        kpi: activeKpi,
        year: String(currentYear),
    });
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
            availableWeeks = normalizeWeeksOrder(payload.weeks_available || []);
            selectedWeeks = normalizeWeeksOrder(payload.weeks_selected || selectedWeeks);
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
            if (vizContainer) {
                vizContainer.innerHTML = "Impossible de recuperer les donnees KPI.";
            }
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
        initGlobalYearSelector();
        initPanelResize();
        checkSnowflakeStatus();
        initFilters();
        fetchKpiData();
        fetchStations();
        fetchGlobalHolidays();
    })
    .catch(() => {
        apiStatus.textContent = "Backend indisponible";
        appendDebugLine("Backend indisponible");
    });

window.addEventListener("resize", () => {
    fetchGlobalHolidays();
});