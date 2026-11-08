(function () {
  function parseTime(value) {
    var parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed / 1000 : null;
  }

  function cssVar(name) {
    return getComputedStyle(document.documentElement)
      .getPropertyValue(name)
      .trim();
  }

  // uPlot draws axis labels, ticks, and grid onto the canvas — CSS cannot reach
  // them. Colors are read from CSS at render time so charts adopt the palette
  // present at render (page load and HTMX fragment re-render).
  function themedAxis() {
    var axisColor = cssVar("--chart-axis");
    var gridColor = cssVar("--chart-grid");
    return {
      stroke: axisColor,
      grid: { stroke: gridColor },
      ticks: { stroke: gridColor }
    };
  }

  // uPlot's canvas carries no accessible name of its own; the headline
  // sentence already lives in the sibling .summary-status live region (see
  // summaryStatus below), so this is a short orienting label, not a repeat
  // of that sentence -- putting the same text in both would double-announce.
  function labelChartCanvas(chart, label) {
    if (!chart || !chart.ctx || !chart.ctx.canvas) {
      return;
    }
    chart.ctx.canvas.setAttribute("role", "img");
    chart.ctx.canvas.setAttribute("aria-label", label);
  }

  function emptyChart(el, text) {
    el.innerHTML = "";
    var node = document.createElement("p");
    node.className = "empty";
    node.textContent = text;
    el.appendChild(node);
    var summary = summaryEl(el);
    if (summary) {
      summaryStatus(summary, text);
      summaryDetail(summary);
    }
  }

  // The chart container's innerHTML is wiped and rebuilt on every render (see
  // emptyChart above and the new uPlot(...) call in each render* function
  // below), but the text/table equivalent must survive that: it lives in a
  // sibling mounted once by the server template (data-summary on the
  // [data-chart] element names its id) and is only ever populated in place,
  // never removed and recreated.
  function summaryEl(el) {
    var id = el.dataset.summary;
    return id ? document.getElementById(id) : null;
  }

  // A screen reader announces a text change inside an aria-live node that
  // was already present, not the appearance of a brand-new node -- so the
  // server template mounts this node once, empty, and every later call only
  // updates its text. The create branch is a fallback for a summary target
  // the template didn't pre-mount one into (id targeted without going
  // through the standard chart-panel markup).
  function summaryStatus(summary, text) {
    var node = summary.querySelector(".summary-status");
    if (!node) {
      node = document.createElement("p");
      node.className = "summary-status visually-hidden";
      node.setAttribute("role", "status");
      node.setAttribute("aria-live", "polite");
      summary.appendChild(node);
    }
    node.textContent = text;
    return node;
  }

  // Companion to summaryStatus: the non-live detail (a collapsible table)
  // that gets replaced wholesale each render, kept in its own child so
  // rebuilding it never touches the persistent live-region node above.
  function summaryDetail(summary) {
    var node = summary.querySelector(".summary-detail");
    if (!node) {
      node = document.createElement("div");
      node.className = "summary-detail";
      summary.appendChild(node);
    }
    node.innerHTML = "";
    return node;
  }

  function appendCell(row, text, isHeader) {
    var cell = document.createElement(isHeader ? "th" : "td");
    if (isHeader) {
      cell.setAttribute("scope", "row");
    }
    cell.textContent = text;
    row.appendChild(cell);
  }

  function appendColumnHeaders(row, labels) {
    labels.forEach(function (text) {
      var th = document.createElement("th");
      th.setAttribute("scope", "col");
      th.textContent = text;
      row.appendChild(th);
    });
  }

  function buildDetailsTable(detail, summaryLabel, captionText) {
    var wrap = document.createElement("details");
    wrap.className = "help";
    var label = document.createElement("summary");
    label.textContent = summaryLabel;
    wrap.appendChild(label);
    var tableWrap = document.createElement("div");
    tableWrap.className = "table-wrap";
    var table = document.createElement("table");
    var caption = document.createElement("caption");
    caption.textContent = captionText;
    table.appendChild(caption);
    tableWrap.appendChild(table);
    wrap.appendChild(tableWrap);
    detail.appendChild(wrap);
    return table;
  }

  function pad2(n) {
    return (n < 10 ? "0" : "") + n;
  }

  // Each missed day's shaded band, drawn under the lines, labelled "missed"
  // or, for a zone changeover day, "zone changeover, not scored". Canvas
  // pixels: valToPos(..., true) and bbox are both in device pixels.
  function drawMissedBands(u, bands) {
    var ratio = window.devicePixelRatio || 1;
    var left = u.bbox.left;
    var right = u.bbox.left + u.bbox.width;
    var ctx = u.ctx;
    ctx.save();
    ctx.font = Math.round(11 * ratio) + "px sans-serif";
    bands.forEach(function (band) {
      var x0 = Math.max(u.valToPos(band.start, "x", true), left);
      var x1 = Math.min(u.valToPos(band.end, "x", true), right);
      if (x1 <= x0) {
        return;
      }
      ctx.globalAlpha = 0.35;
      ctx.fillStyle = cssVar("--chart-grid");
      ctx.fillRect(x0, u.bbox.top, x1 - x0, u.bbox.height);
      ctx.globalAlpha = 1;
      ctx.fillStyle = cssVar("--chart-axis");
      ctx.fillText(band.label, x0 + 4 * ratio, u.bbox.top + 12 * ratio);
    });
    ctx.restore();
  }

  // /api/timeseries also returns the provisional observed line
  // (provisional_valid_at / provisional) and the missed days.
  // The kept pairs, the line and the missed bands share one x axis; a null
  // point at each band's edges breaks every line there, so no line is drawn
  // across a missed day.
  function renderOverlay(el, payload) {
    var pairAt = payload.valid_at || [];
    var provisionalAt = payload.provisional_valid_at || [];
    var missedDays = payload.missed_days || [];
    if (
      !window.uPlot ||
      (pairAt.length === 0 && provisionalAt.length === 0 && missedDays.length === 0)
    ) {
      emptyChart(el, "No overlay pairs.");
      return;
    }
    var forecastRaw = payload.forecast || [];
    var observedRaw = payload.observed || [];
    var provisionalRaw = payload.provisional || [];
    var pairXs = [];
    var forecast = [];
    var observed = [];
    pairAt.forEach(function (value, index) {
      var t = parseTime(value);
      if (t === null) {
        return;
      }
      pairXs.push(t);
      forecast.push(forecastRaw[index] === undefined ? null : forecastRaw[index]);
      observed.push(observedRaw[index] === undefined ? null : observedRaw[index]);
    });
    var points = pairXs.map(function (t, index) {
      return [t, forecast[index], observed[index], null];
    });
    provisionalAt.forEach(function (value, index) {
      var t = parseTime(value);
      if (t === null) {
        return;
      }
      var v = provisionalRaw[index];
      points.push([t, null, null, v === undefined ? null : v]);
    });
    var bands = [];
    missedDays.forEach(function (day) {
      var start = parseTime(day.start);
      var end = parseTime(day.end);
      if (start === null || end === null || end <= start) {
        return;
      }
      bands.push({
        start: start,
        end: end,
        label: day.reason === "zone changeover" ? "zone changeover, not scored" : "missed"
      });
      points.push([start, null, null, null]);
      points.push([end - 1, null, null, null]);
    });
    if (points.length === 0) {
      emptyChart(el, "No overlay pairs.");
      return;
    }
    points.sort(function (a, b) {
      return a[0] - b[0];
    });
    var columns = [[], [], [], []];
    points.forEach(function (point) {
      for (var c = 0; c < 4; c++) {
        columns[c].push(point[c]);
      }
    });
    el.innerHTML = "";
    var chart = new uPlot({
      width: Math.max(el.clientWidth, 320),
      height: el.classList.contains("tall") ? 300 : 220,
      scales: { x: { time: true } },
      axes: [themedAxis(), themedAxis()],
      series: [
        {},
        { label: "Forecast", stroke: cssVar("--chart-1"), width: 2 },
        { label: "Observed", stroke: cssVar("--chart-2"), width: 2 },
        // The third series: the observed colour with a dashed stroke.
        { label: "Observed (provisional)", stroke: cssVar("--chart-2"), width: 2, dash: [6, 4] }
      ],
      hooks: {
        drawClear: [
          function (u) {
            drawMissedBands(u, bands);
          }
        ]
      }
    }, columns, el);
    labelChartCanvas(chart, "Forecast vs. observed overlay chart");
    var summary = summaryEl(el);
    if (summary) {
      // The kept pairs only: the counts and errors describe closed days.
      renderOverlaySummary(summary, payload, pairXs, forecast, observed);
    }
  }

  // Wind is m/s here: /api/timeseries returns the raw stored value, no
  // conversion.
  var VARIABLE_UNITS = {
    temperature: "°C",
    precip: " mm",
    wind: " m/s"
  };

  function roundTo1(value) {
    return Math.round(value * 10) / 10;
  }

  function formatSigned(value, unit) {
    var rounded = roundTo1(value);
    if (rounded > 0) {
      return "+" + rounded.toFixed(1) + unit;
    }
    if (rounded < 0) {
      return rounded.toFixed(1) + unit;
    }
    return "0.0" + unit;
  }

  function biasSentence(value, unit, highWord, lowWord, evenWord) {
    var rounded = roundTo1(value);
    var word = evenWord;
    if (rounded > 0) {
      word = highWord;
    } else if (rounded < 0) {
      word = lowWord;
    }
    return formatSigned(value, unit) + " (" + word + ")";
  }

  function formatLocal(epochSeconds) {
    var d = new Date(epochSeconds * 1000);
    return (
      d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate()) +
      " " + pad2(d.getHours()) + ":" + pad2(d.getMinutes())
    );
  }

  function localDayKey(epochSeconds) {
    var d = new Date(epochSeconds * 1000);
    return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate());
  }

  // The sentences the daily close adds after the pairs' summary, each only
  // when present: the provisional line, the missed days, and the closed days
  // scored under the zone they had then.
  function overlayCloseSentences(payload) {
    var parts = [];
    var hours = [];
    (payload.provisional_valid_at || []).forEach(function (value) {
      var t = parseTime(value);
      if (t !== null) {
        hours.push(t);
      }
    });
    if (hours.length > 0) {
      var latest = payload.provisional_latest_reading_at
        ? parseTime(payload.provisional_latest_reading_at)
        : null;
      parts.push(
        "Provisional observed values from " + formatLocal(hours[0]) + " to " +
          formatLocal(hours[hours.length - 1]) +
          (latest === null ? "" : ", from station readings up to " + formatLocal(latest)) +
          "; they are replaced by the closed values once each day closes."
      );
    }
    var missed = (payload.missed_days || []).map(function (day) {
      return day.local_date;
    });
    if (missed.length > 0) {
      parts.push("Missed, never scored: " + missed.join(", ") + ".");
    }
    var zoned = (payload.zone_days || []).map(function (day) {
      return day.local_date + " (" + day.timezone + ")";
    });
    if (zoned.length > 0) {
      parts.push("Scored under the zone it had then: " + zoned.join(", ") + ".");
    }
    return parts.length > 0 ? " " + parts.join(" ") : "";
  }

  // Computed strictly from xs/forecast/observed -- the same parseTime-
  // filtered arrays renderOverlay just plotted, not payload.valid_at -- so
  // the summary can never describe a row the chart itself dropped. Error
  // statistics (MAE, bias, worst interval) use the subset where both sides
  // are non-null; today /api/timeseries float()-casts both columns so that
  // subset is everything, but the guard stays for a short/malformed array.
  function renderOverlaySummary(summary, payload, xs, forecast, observed) {
    var unit = VARIABLE_UNITS[payload.variable] || "";
    var closeSentences = overlayCloseSentences(payload);
    var pairs = [];
    for (var i = 0; i < xs.length; i++) {
      if (forecast[i] !== null && observed[i] !== null) {
        pairs.push({ t: xs[i], error: forecast[i] - observed[i] });
      }
    }
    if (pairs.length === 0) {
      summaryStatus(summary, "No overlay pairs." + closeSentences);
      summaryDetail(summary);
      return;
    }
    var sumAbs = 0;
    var sum = 0;
    var worstIndex = 0;
    var worstAbs = -1;
    pairs.forEach(function (pair, index) {
      var abs = Math.abs(pair.error);
      sumAbs += abs;
      sum += pair.error;
      if (abs > worstAbs) {
        worstAbs = abs;
        worstIndex = index;
      }
    });
    var mae = sumAbs / pairs.length;
    var bias = sum / pairs.length;
    var worst = pairs[worstIndex];

    // error = forecast - observed, so a positive bias means the forecast
    // ran high.
    summaryStatus(
      summary,
      xs.length + " pair" + (xs.length === 1 ? "" : "s") + " from " + formatLocal(xs[0]) + " to " +
        formatLocal(xs[xs.length - 1]) + ", local time. Mean absolute error " +
        mae.toFixed(1) + unit + ", bias " +
        biasSentence(bias, unit, "forecast ran high", "forecast ran low", "no net bias") +
        ". Worst interval " + formatLocal(worst.t) + ", off by " +
        biasSentence(worst.error, unit, "forecast was high", "forecast was low", "no error") +
        "." + closeSentences
    );

    // Bucketed by browser-local calendar day (renderOverlay's x scale has no
    // tzDate override, so the chart itself is already browser-local) -- a
    // DST fall-back day buckets 25 hourly rows, spring-forward 23; nothing
    // here assumes 24.
    var days = {};
    var dayOrder = [];
    xs.forEach(function (t) {
      var key = localDayKey(t);
      if (!days[key]) {
        days[key] = { count: 0, errors: [] };
        dayOrder.push(key);
      }
      days[key].count += 1;
    });
    pairs.forEach(function (pair) {
      days[localDayKey(pair.t)].errors.push(pair.error);
    });
    var totalDays = dayOrder.length;
    var shown = totalDays > 30 ? dayOrder.slice(totalDays - 30) : dayOrder;
    var captionText = totalDays > 30
      ? "Most recent 30 of " + totalDays + " days, local time."
      : totalDays + " day" + (totalDays === 1 ? "" : "s") + ", local time.";

    var detail = summaryDetail(summary);
    var table = buildDetailsTable(detail, "Per-day breakdown", captionText);
    var thead = document.createElement("thead");
    var headRow = document.createElement("tr");
    appendColumnHeaders(headRow, ["Day", "N", "MAE", "Bias", "Max abs error"]);
    thead.appendChild(headRow);
    table.appendChild(thead);
    var tbody = document.createElement("tbody");
    shown.forEach(function (key) {
      var day = days[key];
      var tr = document.createElement("tr");
      appendCell(tr, key, true);
      appendCell(tr, String(day.count));
      if (day.errors.length === 0) {
        appendCell(tr, "n/a");
        appendCell(tr, "n/a");
        appendCell(tr, "n/a");
      } else {
        var daySumAbs = 0;
        var daySum = 0;
        var dayWorst = 0;
        day.errors.forEach(function (error) {
          var abs = Math.abs(error);
          daySumAbs += abs;
          daySum += error;
          if (abs > dayWorst) {
            dayWorst = abs;
          }
        });
        appendCell(tr, (daySumAbs / day.errors.length).toFixed(1) + unit);
        appendCell(tr, formatSigned(daySum / day.errors.length, unit));
        appendCell(tr, dayWorst.toFixed(1) + unit);
      }
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
  }

  var SKILL_PALETTE = [
    "--chart-1",
    "--chart-2",
    "--chart-3",
    "--chart-4",
    "--chart-5",
    "--chart-6"
  ];

  function leadLabel(value) {
    if (value === 0) {
      return "Today";
    }
    if (value === 1) {
      return "Tomorrow";
    }
    return "+" + value + " days";
  }

  function renderSkill(el, payload) {
    var leads = payload.leads || [];
    var series = payload.series || [];
    // Explicit is-not-null test: 0.0 is a valid eligible point, so a truthiness
    // check would wrongly treat an all-zero-skill series as empty.
    var hasPoint = series.some(function (s) {
      return (s.skill || []).some(function (v) {
        return v !== null && v !== undefined;
      });
    });
    if (!window.uPlot || leads.length === 0 || !hasPoint) {
      emptyChart(el, "No skill curve yet.");
      return;
    }
    var uplotSeries = [{}];
    var data = [leads];
    series.forEach(function (s, index) {
      uplotSeries.push({
        label: s.label,
        stroke: cssVar(SKILL_PALETTE[index % SKILL_PALETTE.length]),
        width: 2,
        spanGaps: false
      });
      data.push(s.skill);
    });
    var xAxis = themedAxis();
    xAxis.values = function (self, splits) {
      return splits.map(leadLabel);
    };
    var yAxis = themedAxis();
    yAxis.label = "Skill";
    el.innerHTML = "";
    var chart = new uPlot({
      width: Math.max(el.clientWidth, 320),
      height: 260,
      scales: { x: { time: false } },
      axes: [xAxis, yAxis],
      series: uplotSeries
    }, data, el);
    labelChartCanvas(chart, "Skill by lead day chart");
    var summary = summaryEl(el);
    if (summary) {
      renderSkillSummary(summary, payload);
    }
  }

  // Naturally tabular already -- one row per lead, one column per feed --
  // so unlike overlay, whose announcement states computed statistics, this
  // one just states the data's shape (leads x feeds) with no headline stat.
  // leadLabel is the same function the chart's own x-axis uses, so the row
  // labels can never drift from what the chart shows.
  // One sentence per feed and day ahead of the dashboard's coverage table
  // whose scored count is below its expected count; "" when the page has no
  // coverage table.
  function coverageSentences() {
    var table = document.getElementById("coverage-table");
    if (!table) {
      return "";
    }
    var parts = [];
    table.querySelectorAll("td[data-day-ahead]").forEach(function (cell) {
      var scored = Number(cell.dataset.scored);
      var expected = Number(cell.dataset.expected);
      if (scored < expected) {
        parts.push(
          cell.dataset.feed + ", D+" + cell.dataset.dayAhead + ": " + scored + " of " +
            expected + " forecasts scored."
        );
      }
    });
    return parts.length > 0 ? " " + parts.join(" ") : "";
  }

  function renderSkillSummary(summary, payload) {
    var leads = payload.leads || [];
    var series = payload.series || [];
    summaryStatus(
      summary,
      leads.length + " lead day" + (leads.length === 1 ? "" : "s") + ", " +
        series.length + " feed" + (series.length === 1 ? "" : "s") + "." +
        coverageSentences()
    );
    var detail = summaryDetail(summary);
    var table = buildDetailsTable(
      detail,
      "Skill data table",
      "Skill by lead day, one row per lead and one column per feed. Higher is better."
    );
    var thead = document.createElement("thead");
    var headRow = document.createElement("tr");
    var leadHeader = document.createElement("th");
    leadHeader.setAttribute("scope", "col");
    leadHeader.textContent = "Lead";
    headRow.appendChild(leadHeader);
    appendColumnHeaders(headRow, series.map(function (s) {
      return s.label;
    }));
    thead.appendChild(headRow);
    table.appendChild(thead);
    var tbody = document.createElement("tbody");
    leads.forEach(function (lead, leadIndex) {
      var tr = document.createElement("tr");
      appendCell(tr, leadLabel(lead), true);
      series.forEach(function (s) {
        var value = (s.skill || [])[leadIndex];
        appendCell(tr, value === null || value === undefined ? "n/a" : value.toFixed(2));
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
  }

  // [data-chart] containers must never carry role="img" or aria-hidden: uPlot
  // mounts a real <table class="u-legend"> inside them, and role="img" is
  // children-presentational, so it would flatten that legend out of the
  // accessibility tree.
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

  // Forecast region (#forecast-tiles): the feed picker, the grids' scroll
  // positions and the day warning tips. A poll or a day click swaps the whole
  // region, so the picker's choice lives here and is applied again after
  // every swap. hiddenFeeds holds the feed ids (data-f / data-feed) whose
  // model rows are hidden.
  var hiddenFeeds = new Set();
  var FORECAST_SCROLLERS = [".tl-wrap", ".mm-wrap .panel"];
  var savedScroll = null;

  // Hides the rows of every hidden feed, ticks the picker to match, and names
  // the choice on its summary and on its Select all / Clear all button.
  function applyFeedPicker(root) {
    root.querySelectorAll("tr[data-feed]").forEach(function (row) {
      row.hidden = hiddenFeeds.has(row.dataset.feed);
    });
    var picker = root.querySelector("details.feed-picker");
    if (!picker) {
      return;
    }
    var boxes = picker.querySelectorAll("input[data-f]");
    var shown = 0;
    boxes.forEach(function (box) {
      box.checked = !hiddenFeeds.has(box.dataset.f);
      shown += box.checked ? 1 : 0;
    });
    var all = shown === boxes.length;
    picker.querySelector("summary").textContent = all
      ? "All feeds"
      : shown === 0 ? "No feeds" : shown + " of " + boxes.length + " feeds";
    var button = picker.querySelector("button.feed-all");
    if (button) {
      button.dataset.all = all ? "none" : "all";
      button.textContent = all ? "Clear all" : "Select all";
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    applyFeedPicker(document);
  });

  document.body.addEventListener("change", function (event) {
    var box = event.target;
    if (!box || !box.matches || !box.matches(".feed-picker input[data-f]")) {
      return;
    }
    if (box.checked) {
      hiddenFeeds.delete(box.dataset.f);
    } else {
      hiddenFeeds.add(box.dataset.f);
    }
    applyFeedPicker(document);
  });

  document.body.addEventListener("click", function (event) {
    var button = event.target && event.target.closest
      ? event.target.closest(".feed-picker button.feed-all")
      : null;
    if (!button) {
      return;
    }
    var clear = button.dataset.all === "none";
    button.closest(".feed-picker").querySelectorAll("input[data-f]").forEach(function (box) {
      if (clear) {
        hiddenFeeds.add(box.dataset.f);
      } else {
        hiddenFeeds.delete(box.dataset.f);
      }
    });
    applyFeedPicker(document);
  });

  // A click outside an open picker closes it, and a click anywhere but on a
  // warning mark closes any open tip; a click on a mark toggles its own tip.
  document.addEventListener("click", function (event) {
    var target = event.target;
    var warn = target && target.closest ? target.closest(".day-warn") : null;
    document.querySelectorAll("details.feed-picker[open]").forEach(function (picker) {
      if (!picker.contains(target)) {
        picker.open = false;
      }
    });
    document.querySelectorAll('.day-warn[aria-expanded="true"]').forEach(function (other) {
      if (other !== warn) {
        other.setAttribute("aria-expanded", "false");
      }
    });
    if (warn) {
      var open = warn.getAttribute("aria-expanded") === "true";
      warn.setAttribute("aria-expanded", open ? "false" : "true");
    }
  });

  // Esc closes every tip, including one shown by hover or focus: .tip-off
  // holds a mark's tip closed until the pointer or focus comes back to it.
  document.addEventListener("keydown", function (event) {
    if (event.key !== "Escape") {
      return;
    }
    document.querySelectorAll(".day-warn").forEach(function (warn) {
      warn.setAttribute("aria-expanded", "false");
      warn.classList.add("tip-off");
    });
  });
  ["mouseover", "focusin"].forEach(function (type) {
    document.addEventListener(type, function (event) {
      var warn = event.target;
      if (warn && warn.classList && warn.classList.contains("day-warn")) {
        warn.classList.remove("tip-off");
      }
    });
  });

  // beforeSwap fires on the old region and afterSettle on the new one, so
  // the timeline's and each grid's scrollLeft are carried across the swap.
  document.body.addEventListener("htmx:beforeSwap", function (event) {
    var region = event.detail.target;
    if (!region || region.id !== "forecast-tiles" || !event.detail.shouldSwap) {
      return;
    }
    savedScroll = FORECAST_SCROLLERS.map(function (selector) {
      return Array.from(region.querySelectorAll(selector), function (el) {
        return el.scrollLeft;
      });
    });
  });

  document.body.addEventListener("htmx:afterSettle", function (event) {
    var region = event.target;
    if (!region || region.id !== "forecast-tiles") {
      return;
    }
    applyFeedPicker(region);
    var saved = savedScroll;
    savedScroll = null;
    if (!saved) {
      return;
    }
    FORECAST_SCROLLERS.forEach(function (selector, i) {
      region.querySelectorAll(selector).forEach(function (el, j) {
        if (j < saved[i].length) {
          el.scrollLeft = saved[i][j];
        }
      });
    });
  });

  // Feed subscription toggles: progress text while a change applies, the
  // fragment guard, and the outcome when no fragment came back. Each Sites
  // page toggle carries data-sub-toggle and an hx-disabled-elt naming every
  // button in its own section, so htmx locks that section while the request
  // is in flight. A success swaps in a fresh section whose status line the
  // server wrote; only the paths that leave the old section in place write
  // the line here.
  var SUB_BUSY = "Applying the change. This can take several minutes; the buttons stay locked until it finishes.";
  var SUB_UNKNOWN = "No answer yet. The change may still be applying. Wait a few minutes, then reload the page to see the current state before clicking again.";
  var SUB_SESSION = "Session expired or request blocked — reload the page to continue";

  function subSection(elt) {
    return elt.closest("section[id^='feed-toggles-']");
  }

  function subLine(section, text) {
    var line = section && section.querySelector("[data-sub-status]");
    if (line) {
      line.textContent = text;
    }
  }

  document.body.addEventListener("htmx:beforeRequest", function (event) {
    var elt = event.detail.elt;
    if (!elt || !elt.matches("[data-sub-toggle]")) {
      return;
    }
    var section = subSection(elt);
    if (section) {
      section.setAttribute("aria-busy", "true");
    }
    subLine(section, SUB_BUSY);
  });

  document.body.addEventListener("htmx:beforeSwap", function (event) {
    // beforeSwap fires on the swap target (the section), not on the button,
    // so the toggle is identified by the element that sent the request.
    var req = event.detail.requestConfig && event.detail.requestConfig.elt;
    if (!req || !req.matches("[data-sub-toggle]")) {
      return;
    }
    // A 2xx body that is not the section (a login page, say) must not land
    // in the card; afterRequest then reports it as no answer.
    var body = String(event.detail.serverResponse || "");
    if (event.detail.shouldSwap && body.indexOf('id="feed-toggles-') === -1) {
      event.detail.shouldSwap = false;
    }
  });

  document.body.addEventListener("htmx:afterRequest", function (event) {
    // After a swap htmx fires this on the detached old button, then on the
    // nearest ancestor still in the page; both are ignored, so a toggle
    // still connected here means its section was not replaced. htmx has
    // already re-enabled the section's buttons by now, so disabling them
    // again below sticks.
    var elt = event.detail.elt;
    if (!elt || !elt.matches("[data-sub-toggle]") || !elt.isConnected) {
      return;
    }
    var section = subSection(elt);
    if (section) {
      section.removeAttribute("aria-busy");
    }
    var xhr = event.detail.xhr;
    var status = xhr ? xhr.status : 0;
    if (status === 403) {
      subLine(section, SUB_SESSION);
      return;
    }
    // Only the add-on's own rejections, raised before anything is written,
    // count as "not changed". Every other outcome (0, 5xx, any other 4xx, a
    // 2xx that was not swapped) may still be applying.
    var message = "";
    if (status === 400 || status === 404 || status === 422) {
      try {
        message = JSON.parse(xhr.responseText).error;
      } catch (e) {
        message = "";
      }
    }
    if (typeof message === "string" && message) {
      subLine(section, "Not changed: " + message + ".");
      return;
    }
    subLine(section, SUB_UNKNOWN);
    if (section) {
      section.querySelectorAll("button").forEach(function (button) {
        button.disabled = true;
      });
    }
  });

  // "Last fetched X ago" stays honest between polls: the tiles fragment
  // answers 204 (no swap) while data is unchanged, so the text is re-derived
  // client-side from the data-updated-at timestamp once a minute.
  function refreshRelativeTimes() {
    document.querySelectorAll("[data-updated-at]").forEach(function (el) {
      var t = Date.parse(el.getAttribute("data-updated-at"));
      if (!Number.isFinite(t)) {
        return;
      }
      var seconds = Math.max(0, (Date.now() - t) / 1000);
      var text;
      if (seconds < 60) {
        text = "just now";
      } else if (seconds < 3600) {
        text = Math.floor(seconds / 60) + " min ago";
      } else if (seconds < 86400) {
        text = Math.floor(seconds / 3600) + " h ago";
      } else {
        text = Math.floor(seconds / 86400) + " d ago";
      }
      el.textContent = "Last fetched: " + text;
    });
  }
  setInterval(refreshRelativeTimes, 60000);

  // Database export: prepare-then-chunked-download. A plain GET download would
  // hold the request open (no headers) through VACUUM INTO and trip HA
  // ingress's response-start timeout, so this POSTs /begin (with CSRF), polls
  // /status until ready, then downloads the retained gz in bounded Range
  // requests and assembles a local Blob. Live capture showed the cutoff is NOT
  // channel-specific: a single long streaming response is cut at ~30 s through
  // Supervisor's ingress proxy on BOTH the navigation and Fetch channels (a
  // 200 was returned to the Fetch while Supervisor logged a stream Connection
  // lost at the same instant). The robust property is that no single response
  // lives long enough to be cut — each ~4 MB Range chunk completes in seconds.
  // The status/download GETs are safe methods and carry no CSRF; begin sends
  // no body/Content-Type so the mutation guard's allowlist is not exercised.
  document.body.addEventListener("click", async function (event) {
    var target = event.target;
    if (!target || !target.matches("#export-run")) {
      return;
    }
    var beginUrl = target.dataset.beginUrl;
    var base = target.dataset.exportBase;
    var result = document.getElementById("export-result");
    function show(text) {
      result.hidden = false;
      result.textContent = text;
    }

    // Generates the same name shape the server assigns at snapshot time
    // (wxverify-<UTC timestamp>Z.db.gz), because the save picker below needs
    // a suggestedName before /begin is even sent -- long before the
    // server's own name exists. The two names differ by the seconds between
    // click and server-side snapshot; that is cosmetic.
    function exportFilename() {
      function pad(n) {
        return (n < 10 ? "0" : "") + n;
      }
      var now = new Date();
      return (
        "wxverify-" +
        now.getUTCFullYear() +
        pad(now.getUTCMonth() + 1) +
        pad(now.getUTCDate()) +
        "-" +
        pad(now.getUTCHours()) +
        pad(now.getUTCMinutes()) +
        pad(now.getUTCSeconds()) +
        "Z.db.gz"
      );
    }

    // The save picker must be called synchronously from the click handler,
    // before /begin: showSaveFilePicker() requires transient user
    // activation, which does not survive the prepare-and-poll wait below
    // (seconds, sometimes minutes). Calling it after that wait works when a
    // developer clicks and waits attentively, and fails in the field with
    // "SecurityError: Must be handling a user gesture".
    var handle = null;
    if (typeof window.showSaveFilePicker === "function") {
      try {
        handle = await window.showSaveFilePicker({
          suggestedName: exportFilename()
        });
      } catch (err) {
        if (err && err.name === "AbortError") {
          result.hidden = true;
          return;
        }
        handle = null;
      }
    }

    // The picker creates (or truncates) the file the instant the user
    // confirms, before /begin is even sent. Every path below where the
    // picker succeeded but the export's bytes never ended up in that file
    // -- prepare failing, the writer never opening, a download failing
    // after it did -- leaves that file behind at 0 bytes, and nothing on
    // this page can remove it (removal needs a directory handle the save
    // picker never returns). The disposition is always the same: name it
    // and say so, never attempt cleanup.
    function emptyFileNote(handle) {
      return handle.name + " is empty; it is safe to delete.";
    }

    function showPrepareFailure(text) {
      if (handle) {
        show(text + " " + emptyFileNote(handle));
      } else {
        show(text);
      }
    }

    var token = document.querySelector('meta[name="csrf-token"]').content;
    show("Preparing export...");
    fetch(beginUrl, {
      method: "POST",
      credentials: "same-origin",
      headers: { "X-CSRF-Token": token }
    })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("begin failed");
        }
        return response.json();
      })
      .then(function (payload) {
        pollStatus(payload.export_id, 0);
      })
      .catch(function () {
        showPrepareFailure("Export failed to start.");
      });

    // 800 polls at 750 ms = 10 minutes. Local prepare (vacuum + gzip) scales
    // to roughly 15 s at production database volume; production storage
    // reads run several times slower than local elsewhere in this project's
    // own measurements, which extrapolates to a prepare time close enough to
    // the previous 180 s budget (240 polls) to risk the client giving up on
    // a preparation the server is still doing.
    var MAX_POLLS = 800;
    function pollStatus(exportId, attempts) {
      if (attempts >= MAX_POLLS) {
        showPrepareFailure("Export timed out.");
        return;
      }
      fetch(base + "/status/" + exportId, { credentials: "same-origin" })
        .then(function (response) {
          if (!response.ok) {
            throw new Error("status failed");
          }
          return response.json();
        })
        .then(function (payload) {
          if (payload.state === "ready") {
            // Deliberately not awaited: this call lives inside a .then(),
            // and the .catch() below renders "Export failed." on rejection,
            // which would clobber the fallback link if a download failure
            // surfaced here. Safe only because triggerDownload never
            // rejects -- every await inside it is guarded by its own
            // try/catch, leaving only synchronous DOM updates unguarded.
            triggerDownload(
              base + "/download/" + exportId,
              payload.size,
              handle
            );
            show("Download started.");
          } else if (payload.state === "error") {
            showPrepareFailure("Export failed.");
          } else {
            window.setTimeout(function () {
              pollStatus(exportId, attempts + 1);
            }, 750);
          }
        })
        .catch(function () {
          showPrepareFailure("Export failed.");
        });
    }

    function formatBytes(bytes) {
      return (bytes / 1048576).toFixed(1) + " MB";
    }

    // The route sets Content-Disposition: attachment; the filename is
    // wxverify-<UTC timestamp>Z.db.gz (timestamp %Y%m%d-%H%M%S, not ISO-8601).
    // Falls back to a stable name if the header is absent or unparseable.
    function parseFilename(response) {
      var fallback = "wxverify-export.db.gz";
      var header = response.headers.get("Content-Disposition");
      if (!header) {
        return fallback;
      }
      var match = /filename\*?=(?:UTF-8'')?"?([^";]+)"?/i.exec(header);
      if (!match || !match[1]) {
        return fallback;
      }
      try {
        return decodeURIComponent(match[1]);
      } catch (err) {
        return match[1];
      }
    }

    // Saves an in-memory Blob via a transient <a download>. The object URL is
    // revoked on a delayed tick: revoking synchronously right after click can
    // cancel the save in some browsers.
    function saveBlob(blob, filename) {
      var url = URL.createObjectURL(blob);
      var anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = filename;
      document.body.appendChild(anchor);
      anchor.click();
      document.body.removeChild(anchor);
      window.setTimeout(function () {
        URL.revokeObjectURL(url);
      }, 60000);
    }

    // Retained-file fallback (0.8.3 safety net): on any fetch/read failure,
    // render a clickable browser-native download link into the result area so
    // the user can still download + Retry against the retained export file.
    function showFallbackLink(message, downloadUrl, filename) {
      result.hidden = false;
      result.textContent = "";
      result.appendChild(document.createTextNode(message + " "));
      var link = document.createElement("a");
      link.href = downloadUrl;
      link.download = filename;
      link.rel = "noopener";
      link.textContent = "Download database";
      result.appendChild(link);
    }

    // Bounded-Range chunked download: fetch the retained gz in sequential
    // ~4 MB Range requests, each of which completes in a few seconds (far under
    // the ~30 s ingress cutoff), then assemble the parts into one local Blob.
    // No single response lives long enough to be cut, and a ~4 MB chunk also
    // stays under Supervisor's streaming threshold (~4,194,000 bytes) so each
    // takes the buffered/simple-response path — but correctness does not depend
    // on that exact constant. Needs the total byte size (from the ready status
    // payload) to compute chunk boundaries; if it is missing or zero, fall back
    // to the retained-file link rather than guessing. Any per-chunk failure
    // (non-206, wrong length, network error) retries that chunk up to 3 times,
    // then falls through to showFallbackLink (the 0.8.3 retained file + the
    // browser's native download / Firefox Retry still survive).
    async function triggerDownload(downloadUrl, totalSize, handle) {
      var total = Number(totalSize);
      if (!Number.isFinite(total) || total <= 0) {
        var sizeUnknownMessage = "Export ready, but its size is unknown.";
        if (handle) {
          sizeUnknownMessage += " " + emptyFileNote(handle);
        }
        showFallbackLink(
          sizeUnknownMessage + " Use this link to save it:",
          downloadUrl,
          "wxverify-export.db.gz"
        );
        return;
      }
      var CHUNK_SIZE = 4000000;
      var filename = "wxverify-export.db.gz";
      var parts = [];
      var received = 0;
      show("Downloading...");

      // Guarded independently of the picker above: createWritable() can
      // reject even after a successful pick (a revoked permission, a locked
      // file, a full disk). A rejection here falls back to the in-memory
      // Blob path rather than losing the export, but the user already chose
      // a destination that will not receive it, so this says so rather than
      // silently downgrading.
      var writable = null;
      if (handle) {
        try {
          writable = await handle.createWritable();
        } catch (err) {
          writable = null;
          show(
            "Could not open the chosen file for writing. " +
              emptyFileNote(handle) +
              " Downloading instead..."
          );
        }
      }

      // Aborts the writer (if one exists) before rendering the fallback, so
      // a failure never leaves a partially-written file that looks like a
      // complete backup -- createWritable() writes to a swap file that only
      // replaces the destination at close(), so an abort discards that swap
      // file; the destination never received the partial bytes.
      async function fail() {
        if (writable) {
          try {
            await writable.abort();
          } catch (abortErr) {
            // Best-effort: still render the fallback even if the abort
            // itself fails, so a failure here can never suppress the one
            // message the user has left to recover the export.
          }
          showFallbackLink(
            "Download failed. " +
              emptyFileNote(handle) +
              " Use this link to save the retained file:",
            downloadUrl,
            filename
          );
          return;
        }
        showFallbackLink(
          "Download failed. Use this link to save the retained file:",
          downloadUrl,
          filename
        );
      }

      // Fetch [start, end] as a 206 Range request, retrying that chunk up to
      // `attemptsLeft` total tries before rejecting. Resolves the chunk's
      // ArrayBuffer once its status, Content-Range, and byte length all check.
      function fetchChunk(start, attemptsLeft) {
        var end = Math.min(start + CHUNK_SIZE - 1, total - 1);
        var expected = end - start + 1;
        return fetch(downloadUrl, {
          credentials: "same-origin",
          headers: { Range: "bytes=" + start + "-" + end }
        })
          .then(function (response) {
            if (response.status !== 206) {
              throw new Error("expected 206, got " + response.status);
            }
            var contentRange = response.headers.get("Content-Range");
            if (!contentRange) {
              throw new Error("missing Content-Range");
            }
            var m = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(contentRange);
            if (!m) {
              throw new Error("bad Content-Range: " + contentRange);
            }
            if (Number(m[1]) !== start || Number(m[3]) !== total) {
              throw new Error(
                "Content-Range " + contentRange + " != bytes " +
                  start + "-" + end + "/" + total
              );
            }
            if (start === 0) {
              filename = parseFilename(response);
            }
            return response.arrayBuffer();
          })
          .then(function (buffer) {
            if (buffer.byteLength !== expected) {
              throw new Error(
                "chunk length " + buffer.byteLength + " != " + expected
              );
            }
            return buffer;
          })
          .catch(function (err) {
            if (attemptsLeft > 1) {
              return fetchChunk(start, attemptsLeft - 1);
            }
            throw err;
          });
      }

      // Sequential chunk loop, mirroring the recursive pump() reader: each
      // chunk is fetched only after the previous one lands, so at most one
      // chunk is in flight and only one failure path can fire. The
      // recursive call is returned rather than fire-and-forget, so a
      // rejection at any depth -- a write, the completeness check, or
      // close() itself -- propagates to the try/catch below instead of
      // becoming an unhandled rejection.
      async function nextChunk(start) {
        if (start >= total) {
          if (writable) {
            if (received !== total) {
              throw new Error("received " + received + " != " + total);
            }
            await writable.close();
            show("Download complete. Saved.");
            return;
          }
          var blob = new Blob(parts, { type: "application/gzip" });
          if (blob.size !== total) {
            throw new Error("blob size " + blob.size + " != " + total);
          }
          saveBlob(blob, filename);
          if (handle) {
            show(
              "Download complete. Saved to your downloads folder instead. " +
                emptyFileNote(handle)
            );
          } else {
            show("Download complete. Saved.");
          }
          return;
        }
        var buffer = await fetchChunk(start, 3);
        if (writable) {
          await writable.write(buffer);
        } else {
          parts.push(buffer);
        }
        received += buffer.byteLength;
        show(
          "Downloading... " +
            formatBytes(received) +
            " / " +
            formatBytes(total)
        );
        return nextChunk(start + CHUNK_SIZE);
      }

      try {
        await nextChunk(0);
      } catch (err) {
        await fail();
      }
    }
  });

  // Full-page navigation feedback. Clicking a nav or filter link leaves the
  // CURRENT document on screen until the server responds; on the slow pages
  // that is seconds with no sign anything is happening. The browser cannot
  // blank the page early -- blanking requires the REPLACEMENT document to
  // start rendering -- so the indicator has to be drawn by the page being
  // left, and torn down by every path that comes back to it.
  var NAV_DELAY_MS = 150; // under this a navigation reads as instant
  var NAV_MAX_MS = 30000; // ingress cuts a single response at ~30 s
  var navShowTimer = null;
  var navHideTimer = null;

  function navBar() {
    var el = document.getElementById("nav-progress");
    if (el) {
      return el;
    }
    el = document.createElement("div");
    el.id = "nav-progress";
    el.setAttribute("role", "status");
    el.setAttribute("aria-live", "polite");
    el.hidden = true;
    var label = document.createElement("span");
    label.className = "visually-hidden";
    label.textContent = "Loading";
    el.appendChild(label);
    document.body.appendChild(el);
    return el;
  }

  function navClear() {
    window.clearTimeout(navShowTimer);
    window.clearTimeout(navHideTimer);
    navShowTimer = null;
    navHideTimer = null;
    var el = document.getElementById("nav-progress");
    if (el) {
      el.hidden = true;
    }
  }

  function navStart() {
    navClear();
    navShowTimer = window.setTimeout(function () {
      navBar().hidden = false;
    }, NAV_DELAY_MS);
    navHideTimer = window.setTimeout(navClear, NAV_MAX_MS);
  }

  // True only when the click will actually replace this document. Everything
  // else -- new tab, download, external host, in-page anchor, htmx swap --
  // leaves the document in place, so a bar shown for it would never be torn
  // down by a load.
  function navigatesAway(event, a) {
    if (event.defaultPrevented || event.button !== 0) {
      return false;
    }
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
      return false;
    }
    if (a.hasAttribute("download") || a.hasAttribute("target")) {
      return false;
    }
    // No anchor carries hx-* today; this keeps it true by construction rather
    // than by convention, so an htmx fragment swap can never raise the bar.
    var attrs = a.getAttributeNames();
    for (var i = 0; i < attrs.length; i++) {
      if (attrs[i].indexOf("hx-") === 0) {
        return false;
      }
    }
    // mailto:/tel:/javascript: resolve to an opaque origin, so this one check
    // covers non-http schemes as well as other hosts.
    if (a.origin !== window.location.origin) {
      return false;
    }
    // Same path+search plus a fragment is a scroll, not a load. A link to the
    // identical URL with no fragment IS a reload and does get the bar.
    return !(
      a.hash &&
      a.pathname === window.location.pathname &&
      a.search === window.location.search
    );
  }

  document.body.addEventListener("click", function (event) {
    var target = event.target;
    if (!target || !target.closest) {
      return;
    }
    var a = target.closest("a[href]");
    if (a && navigatesAway(event, a)) {
      navStart();
    }
  });

  // A bfcache restore replays this document with the bar still visible, so
  // pageshow must clear on EVERY restore. Without it, Back leaves a spinner
  // that never goes away.
  window.addEventListener("pageshow", navClear);

  // Esc cancels a pending navigation in every major browser but fires no
  // event of its own; clear on the keypress so the bar does not outlive it.
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") {
      navClear();
    }
  });
})();
