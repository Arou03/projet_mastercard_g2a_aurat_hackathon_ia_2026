const API_URL = "https://projet-mastercard-g2a-aurat-hackathon-ia.onrender.com";

const params = new URLSearchParams(window.location.search);
const dep = params.get("dep");

// THEME MANAGEMENT
function initThemeToggle() {
    const themeToggle = document.getElementById("themeToggle");
    if (!themeToggle) return;
    
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

const title = document.getElementById("title");
const frequentationValue = document.getElementById("frequentationValue");
const insightsList = document.getElementById("insightsList");

function formatNumber(value) {
    if (typeof value !== "number" || Number.isNaN(value)) {
        return "-";
    }
    return new Intl.NumberFormat("fr-FR").format(value);
}

function renderError(message) {
    title.textContent = "Erreur";
    insightsList.innerHTML = `<li>${message}</li>`;
}

initThemeToggle();

if (!dep) {
    renderError("Aucun departement fourni dans l'URL.");
} else {
    title.textContent = `Departement : ${dep}`;

    fetch(`${API_URL}/api/department/${encodeURIComponent(dep)}`)
        .then(res => {
            if (!res.ok) {
                throw new Error("Departement introuvable");
            }
            return res.json();
        })
        .then(payload => {
            title.textContent = `Departement : ${payload.name}`;
            frequentationValue.textContent = formatNumber(payload.kpis.frequentation);
            insightsList.innerHTML = payload.insights.map(item => `<li>${item}</li>`).join("");
        })
        .catch(() => {
            renderError("Impossible de charger les donnees de ce departement.");
        });
}
