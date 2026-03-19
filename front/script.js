const API_URL = "https://projet-mastercard-g2a-aurat-hackathon-ia.onrender.com"; 
// ⚠️ Remplace par l'URL réelle de ton backend Render

function callAPI() {
    fetch(`${API_URL}/api/hello`)
        .then(response => response.json())
        .then(data => {
            document.getElementById("result").innerText = data.message;
        })
        .catch(error => {
            console.error("Error:", error);
            document.getElementById("result").innerText = "Error calling API";
        });
}