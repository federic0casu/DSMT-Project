const violationEventSocket = new WebSocket('ws://' + host + '/web-app/events/violations');
const inactivityViolationsTableBody = document.querySelector("#inactivityViolationsTable tbody");
const speedingViolationsTableBody = document.querySelector("#speedingViolationsTable tbody");

violationEventSocket.onopen = function (event) {
    console.log("WebSocket OPENED (url='ws://" + host + "/web-app/events/violations')");
};

violationEventSocket.onmessage = function (event) {
    const violationData = JSON.parse(event.data);

    // Controlla se il messaggio è una violazione di inattività
    if (violationData.tsLastActivity!=0 && (violationData.userSpeed==0 && !violationData.speedLimit)) {
        addRowToTable(inactivityViolationsTableBody, [
            violationData.vin,
            formatTimestamp(violationData.violationTs),
            formatTimestamp(violationData.tsLastActivity)
        ]);
    } else {
        // Gestisci le violazioni di eccesso di velocità
        addRowToTable(speedingViolationsTableBody, [
            violationData.vin,
            formatTimestamp(violationData.violationTs),
            `${violationData.userSpeed} km/h`,
            violationData.speedLimit ? `${violationData.speedLimit.maxSpeed} km/h` : "N/A",
            violationData.speedLimit ? violationData.speedLimit.wayName : "N/A"
        ]);
    }
};

function addRowToTable(tableBody, cellValues) {
    let newRow = document.createElement("tr");
    newRow.innerHTML = cellValues.map(value => `<td>${value}</td>`).join('');
    tableBody.insertBefore(newRow, tableBody.firstChild);
    
    if (tableBody.children.length > MAX_ROWS) {
        tableBody.removeChild(tableBody.lastChild);
    }
}

function formatTimestamp(timestamp) {
    const date = new Date(timestamp);
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}:${String(date.getSeconds()).padStart(2, '0')}`;
}
