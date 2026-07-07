(function () {
  function parseTime(value) {
    var parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed / 1000 : 0;
  }

  function emptyChart(el, text) {
    el.innerHTML = "";
    var node = document.createElement("p");
    node.className = "empty";
    node.textContent = text;
    el.appendChild(node);
  }

  function renderOverlay(el, payload) {
    if (!payload.valid_at || payload.valid_at.length === 0 || !window.uPlot) {
      emptyChart(el, "No overlay pairs.");
      return;
    }
    var x = payload.valid_at.map(parseTime);
    var forecast = payload.forecast || [];
    var observed = payload.observed || [];
    el.innerHTML = "";
    new uPlot({
      width: Math.max(el.clientWidth, 320),
      height: el.classList.contains("tall") ? 300 : 220,
      scales: { x: { time: true } },
      series: [
        {},
        { label: "Forecast", stroke: "#2563eb", width: 2 },
        { label: "Observed", stroke: "#0f766e", width: 2 }
      ]
    }, [x, forecast, observed], el);
  }

  function renderSkill(el, payload) {
    var rows = payload.rows || [];
    var usable = rows.filter(function (row) {
      return row.skill_score !== null && row.skill_score !== undefined;
    });
    if (usable.length === 0 || !window.uPlot) {
      emptyChart(el, "No skill curve rows.");
      return;
    }
    var x = usable.map(function (row, index) { return index; });
    var y = usable.map(function (row) { return row.skill_score; });
    el.innerHTML = "";
    new uPlot({
      width: Math.max(el.clientWidth, 320),
      height: 220,
      series: [
        {},
        { label: "Skill", stroke: "#2563eb", width: 2 }
      ]
    }, [x, y], el);
  }

  function loadChart(el) {
    if (el.dataset.loaded === "true") {
      return;
    }
    el.dataset.loaded = "true";
    fetch(el.dataset.src, { credentials: "same-origin" })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("chart fetch failed");
        }
        return response.json();
      })
      .then(function (payload) {
        if (el.dataset.chart === "overlay") {
          renderOverlay(el, payload);
        } else {
          renderSkill(el, payload);
        }
      })
      .catch(function () {
        emptyChart(el, "Chart data unavailable.");
      });
  }

  function bootCharts(root) {
    root.querySelectorAll("[data-chart][data-src]").forEach(loadChart);
  }

  document.addEventListener("DOMContentLoaded", function () {
    bootCharts(document);
  });
  document.body.addEventListener("htmx:afterSettle", function (event) {
    bootCharts(event.target);
  });
})();
