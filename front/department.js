const API_URL = "https://projet-mastercard-g2a-aurat-hackathon-ia.onrender.com";

const params = new URLSearchParams(window.location.search);
const dep = params.get("dep");

const title = document.getElementById("title");
const frequentationValue = document.getElementById("frequentationValue");
const meteoValue = document.getElementById("meteoValue");
const securiteValue = document.getElementById("securiteValue");
const insightsList = document.getElementById("insightsList");

function renderError(message) {
    title.textContent = "Erreur";
    insightsList.innerHTML = `<li>${message}</li>`;
}

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
            frequentationValue.textContent = payload.kpis.frequentation;
            meteoValue.textContent = payload.kpis.meteo;
            securiteValue.textContent = payload.kpis.securite;
            insightsList.innerHTML = payload.insights.map(item => `<li>${item}</li>`).join("");
        })
        .catch(() => {
            renderError("Impossible de charger les donnees de ce departement.");
        });
}
