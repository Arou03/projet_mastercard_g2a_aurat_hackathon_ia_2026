const API_URL = "https://projet-mastercard-g2a-aurat-hackathon-ia.onrender.com";

const params = new URLSearchParams(window.location.search);
const dep = params.get("dep");

const title = document.getElementById("title");
const timelineStatus = document.getElementById("timelineStatus");
const timelineCanvas = document.getElementById("timelineChart");
const holidayCanvas = document.getElementById("holidayTimeline");
const timelineLegend = document.getElementById("timelineLegend");
const holidayLegend = document.getElementById("holidayLegend");
const errorBox = document.getElementById("departmentError");
const yearSelector = document.getElementById("yearSelector");
const yearInfo = document.getElementById("yearInfo");

let currentYear = 2024;

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

function initYearSelector() {
    if (!yearSelector) return;

    yearSelector.addEventListener("change", (e) => {
        currentYear = parseInt(e.target.value, 10);
        updateYearInfo();
        loadDepartmentData();
    });
}

function updateYearInfo() {
    if (!yearInfo) return;
    if (currentYear === 2024) {
        yearInfo.textContent = "Données observées dans FREQ_GLOBAL_PER_DEPT";
    } else {
        yearInfo.textContent = `Prédiction ML (croissance 5% par an)`;  
    }
}

function formatNumber(value) {
    if (typeof value !== "number" || Number.isNaN(value)) {
        return "-";
    }
    return new Intl.NumberFormat("fr-FR").format(value);
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

function getCanvasSize(canvas) {
    const ratio = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    const width = Math.max(320, Math.floor(rect.width));
    const height = Math.max(200, Math.floor(rect.height));
    canvas.width = Math.floor(width * ratio);
    canvas.height = Math.floor(height * ratio);
    const ctx = canvas.getContext("2d");
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    return { ctx, width, height };
}

function toWeekNumber(weekLabel) {
    const text = String(weekLabel || "").toUpperCase().trim();
    if (text.startsWith("S") && /^\d+$/.test(text.slice(1))) {
        return Number.parseInt(text.slice(1), 10);
    }
    return null;
}

function toAxisRank(weekLabel) {
    const number = toWeekNumber(weekLabel);
    if (number === null) {
        return 999;
    }
    if (number === 51) {
        return 0;
    }
    if (number === 52) {
        return 1;
    }
    if (number >= 1 && number <= 15) {
        return number + 1;
    }
    return number + 200;
}

function drawTimelineChart(weeks, series) {
    const { ctx, width, height } = getCanvasSize(timelineCanvas);
    const pad = { top: 20, right: 20, bottom: 36, left: 58 };
    const chartW = width - pad.left - pad.right;
    const chartH = height - pad.top - pad.bottom;
    const allValues = series
        .flatMap(item => item.values || [])
        .filter(value => typeof value === "number" && !Number.isNaN(value));
    const minValue = allValues.length ? Math.min(...allValues) : 0;
    const maxValue = allValues.length ? Math.max(...allValues) : 100;
    const range = Math.max(1, maxValue - minValue);

    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = "#f8fbff";
    ctx.fillRect(0, 0, width, height);

    ctx.strokeStyle = "#d4dfe9";
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i += 1) {
        const y = pad.top + (chartH * i) / 4;
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(width - pad.right, y);
        ctx.stroke();
    }

    const xFromIndex = index => {
        if (weeks.length <= 1) {
            return pad.left;
        }
        return pad.left + (index / (weeks.length - 1)) * chartW;
    };

    const yFromValue = value => pad.top + chartH - ((value - minValue) / range) * chartH;

    series.forEach(line => {
        const values = line.values || [];
        ctx.strokeStyle = line.color || "#086cb2";
        ctx.lineWidth = line.id === "observed" ? 2.5 : 1.6;
        ctx.setLineDash(line.id === "prediction" ? [6, 4] : []);

        let started = false;
        values.forEach((value, index) => {
            if (typeof value !== "number" || Number.isNaN(value)) {
                return;
            }
            const x = xFromIndex(index);
            const y = yFromValue(value);
            if (!started) {
                ctx.beginPath();
                ctx.moveTo(x, y);
                started = true;
            } else {
                ctx.lineTo(x, y);
            }
        });
        if (started) {
            ctx.stroke();
        }
        ctx.setLineDash([]);
    });

    ctx.fillStyle = "#162c4a";
    ctx.font = "12px Luciole, Segoe UI, sans-serif";
    const tickIndexes = [0, Math.floor((weeks.length - 1) / 3), Math.floor(((weeks.length - 1) * 2) / 3), Math.max(0, weeks.length - 1)];
    const seen = new Set();
    tickIndexes.forEach(index => {
        if (seen.has(index) || !weeks[index]) {
            return;
        }
        seen.add(index);
        const x = xFromIndex(index);
        ctx.textAlign = "center";
        ctx.fillText(weeks[index], x, height - 12);
    });

    ctx.textAlign = "right";
    ctx.fillText(formatNumber(minValue), pad.left - 8, pad.top + chartH + 4);
    ctx.fillText(formatNumber(maxValue), pad.left - 8, pad.top + 4);

    timelineLegend.innerHTML = series
        .map(item => `<span class="legend-chip"><span class="legend-dot" style="background:${item.color}"></span>${item.label}</span>`)
        .join("");
}

function drawHolidayLanes(weeks, holidays, countries) {
    const { ctx, width, height } = getCanvasSize(holidayCanvas);
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

    const xFromWeek = weekLabel => {
        const rank = toAxisRank(weekLabel);
        return pad.left + ((rank - minRank) / rankSpan) * chartW;
    };

    orderedCountries.forEach((country, row) => {
        const y = pad.top + row * laneHeight;
        ctx.fillStyle = row % 2 === 0 ? "#eef5fb" : "#e5eff8";
        ctx.fillRect(pad.left, y, chartW, laneHeight - 2);
        ctx.fillStyle = "#162c4a";
        ctx.font = "12px Luciole, Segoe UI, sans-serif";
        ctx.textAlign = "right";
        ctx.fillText(country, pad.left - 10, y + laneHeight / 2 + 4);
    });

    holidays.forEach(item => {
        const row = Math.max(0, orderedCountries.indexOf(item.country_name || item.country_code || ""));
        const y = pad.top + row * laneHeight + 3;
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
        const color = holidayColor(`${item.country_code}:${item.season}:${item.holiday_type}`);
        ctx.fillStyle = color;
        ctx.globalAlpha = 0.35;
        ctx.fillRect(x1, y, Math.max(3, x2 - x1), Math.max(12, laneHeight - 6));
        ctx.globalAlpha = 1;
    });

    ctx.fillStyle = "#162c4a";
    ctx.font = "12px Luciole, Segoe UI, sans-serif";
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
    holidayLegend.innerHTML = holidayTypes.length
        ? holidayTypes.map(type => `<span class="legend-chip">${type}</span>`).join("")
        : "Aucune periode de vacances disponible";
}

function renderError(message) {
    errorBox.classList.remove("hidden");
    errorBox.textContent = message;
}

function renderTimeline(payload) {
    const timeline = payload.timeline || {};
    const weeks = timeline.weeks || [];
    const series = timeline.series || [];
    const holidays = timeline.holidays || [];
    const countries = timeline.countries || [];

    if (!weeks.length || !series.length) {
        renderError("Aucune donnee temporelle disponible pour ce departement.");
        return;
    }

    drawTimelineChart(weeks, series);
    drawHolidayLanes(weeks, holidays, countries);
}

function loadDepartmentData() {
    if (!dep) {
        renderError("Aucun departement fourni dans l'URL.");
        return;
    }
    
    timelineStatus.textContent = "Chargement des donnees...";
    errorBox.classList.add("hidden");

    const url = `${API_URL}/api/department/${encodeURIComponent(dep)}/timeline?year=${currentYear}`;
    
    fetch(url)
        .then(res => {
            if (!res.ok) {
                throw new Error("Timeline indisponible");
            }
            return res.json();
        })
        .then(payload => {
            title.textContent = `Departement : ${payload.name}`;
            timelineStatus.textContent = `Annee ${currentYear} - Source: ${payload.data_source || "inconnue"}`;
            renderTimeline(payload);
        })
        .catch(() => {
            renderError("Impossible de charger les visualisations temporelles.");
        });
}

initThemeToggle();
initYearSelector();
updateYearInfo();

if (dep) {
    title.textContent = `Departement : ${dep}`;
    loadDepartmentData();
}

window.addEventListener("resize", () => {
    if (dep) {
        loadDepartmentData();
    }
});
