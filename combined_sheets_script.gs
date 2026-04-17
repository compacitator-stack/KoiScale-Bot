// ============================================================
// GreenClaw + KoiScale + RossWatcher — Google Apps Script
// ============================================================
// SETUP:
// 1. Open your existing "GreenClaw Trading Log" spreadsheet
// 2. Extensions > Apps Script > REPLACE the existing script with this file > Save
// 3. Deploy > Manage deployments > Edit (pencil icon) > Version: New version > Deploy
//    (This updates the SAME webhook URL — no env var changes needed)
// ============================================================

var SHEETS = {
  // GreenClaw tabs (unchanged)
  summary:    "Summary",
  trades:     "Trades",
  eod:        "Daily P&L",
  scan:       "Scan Log",
  ross:       "Ross Insights",
  // KoiScale tabs (new)
  koi_trades: "KoiScale Trades",
  koi_eod:    "KoiScale Daily P&L",
  koi_summary:"KoiScale Summary"
};

// ── Entry point ──────────────────────────────────────────────
function doPost(e) {
  try {
    var data = JSON.parse(e.postData.contents);
    var type = data.type || "unknown";
    var bot  = data.bot_version || "";
    var isKoi = bot.indexOf("KoiScale") !== -1;

    if (isKoi) {
      // ── KoiScale routing ──────────────────────────────────
      if      (type === "trade_close") { handleKoiTrade(data); }
      else if (type === "eod")         { handleKoiEOD(data); }
      updateKoiSummary();
    } else {
      // ── GreenClaw routing (unchanged) ─────────────────────
      if      (type === "trade")        { handleTrade(data); }
      else if (type === "eod")          { handleEOD(data); }
      else if (type === "scan")         { handleScan(data); }
      else if (type === "ross_insight") { handleRossInsight(data); }
      updateSummary();
    }

    return ContentService
      .createTextOutput(JSON.stringify({ ok: true, type: type, bot: bot }))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    return ContentService
      .createTextOutput(JSON.stringify({ ok: false, error: err.message }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

// ================================================================
//  KOISCALE HANDLERS
// ================================================================

// ── KoiScale Trade handler ───────────────────────────────────
function handleKoiTrade(d) {
  var sheet = getOrCreateSheet(SHEETS.koi_trades, [
    "Date", "Time", "Symbol", "Outcome", "Entry $", "Stop $",
    "Target 1 $", "Target 2 $", "R:R", "VWAP $",
    "Confluence", "Choppy", "Bot Version"
  ]);

  sheet.appendRow([
    formatDate(d.timestamp),
    formatTime(d.timestamp),
    d.symbol       || "",
    d.outcome      || "",
    d.entry        || "",
    d.stop_1       || "",
    d.target_1     || "",
    d.target_2     || "",
    d.rr           || "",
    d.vwap         || "",
    d.confluence   || 0,
    d.choppy ? "Yes" : "No",
    d.bot_version  || ""
  ]);

  formatKoiTradesSheet(sheet);
}

// ── KoiScale EOD handler ─────────────────────────────────────
function handleKoiEOD(d) {
  var sheet = getOrCreateSheet(SHEETS.koi_eod, [
    "Date", "Day", "Outcome", "Trades", "P&L $", "Equity $",
    "Prev Equity $", "Candidates", "Choppy Mode", "Strategy",
    "Running P&L $"
  ]);

  var lastRow    = sheet.getLastRow();
  var runningPnl = 0;
  if (lastRow > 1) {
    var prevRunning = sheet.getRange(lastRow, 11).getValue();
    runningPnl = (Number(prevRunning) || 0) + (Number(d.pnl) || 0);
  } else {
    runningPnl = Number(d.pnl) || 0;
  }

  sheet.appendRow([
    d.date         || "",
    d.day_name     || "",
    d.outcome      || "",
    d.trades       || 0,
    d.pnl          || 0,
    d.equity       || 0,
    d.last_equity  || 0,
    d.candidates   || 0,
    d.choppy_mode ? "Yes" : "No",
    d.strategy     || "",
    runningPnl
  ]);

  formatKoiEODSheet(sheet);
}

// ── KoiScale Summary ─────────────────────────────────────────
function updateKoiSummary() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEETS.koi_summary);
  if (!sheet) {
    sheet = ss.insertSheet(SHEETS.koi_summary);
  }
  sheet.clearContents();

  var stats     = calcKoiStats();
  var eodSheet  = ss.getSheetByName(SHEETS.koi_eod);
  var greenDays = eodSheet ? countGreenDays(eodSheet) : 0;
  var totalDays = eodSheet ? Math.max(eodSheet.getLastRow() - 1, 0) : 0;
  var greenPct  = totalDays > 0
    ? (greenDays / totalDays * 100).toFixed(1) + "%" : "--";

  var data = [
    ["KoiScale Performance Summary", ""],
    ["Last updated", new Date().toLocaleString()],
    ["", ""],
    ["-- Paper Trading Stats --", ""],
    ["Total trades",       stats.totalTrades],
    ["Wins",               stats.wins],
    ["Win rate",           stats.winRate + "%"],
    ["Avg R:R",            stats.avgRR],
    ["Total P&L",          "$" + stats.totalPnl.toFixed(2)],
    ["Current equity",     "$" + stats.equity.toFixed(2)],
    ["", ""],
    ["-- Daily Stats --", ""],
    ["Trading days",       totalDays],
    ["Green days",         greenDays],
    ["Red days",           totalDays - greenDays],
    ["Green day rate",     greenPct],
    ["", ""],
    ["-- Strategy --", ""],
    ["Type",               "VWAP Mean Reversion (Brian Shannon)"],
    ["Stage-2 filter",     "Yes"],
    ["Choppy mode",        "Adaptive sizing"]
  ];

  sheet.getRange(1, 1, data.length, 2).setValues(data);
  sheet.getRange(1, 1, 1, 2).setFontWeight("bold").setFontSize(14);
  [4, 12, 18].forEach(function(r) {
    sheet.getRange(r, 1).setFontWeight("bold");
  });
  sheet.setColumnWidth(1, 260);
  sheet.setColumnWidth(2, 220);

  var wr     = parseFloat(stats.winRate);
  var wrCell = sheet.getRange(7, 2);
  if      (wr >= 65) { wrCell.setBackground("#b7e1cd"); }
  else if (wr >= 50) { wrCell.setBackground("#fce8b2"); }
  else               { wrCell.setBackground("#f4c7c3"); }
}

function calcKoiStats() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEETS.koi_trades);
  if (!sheet || sheet.getLastRow() < 2) {
    return { totalTrades: 0, wins: 0, winRate: "0.0",
             avgRR: "0.0", totalPnl: 0, equity: 0 };
  }

  var data = sheet.getRange(
    2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()
  ).getValues();

  // Columns: Date(0) Time(1) Symbol(2) Outcome(3) Entry(4) Stop(5)
  //          T1(6) T2(7) RR(8) VWAP(9) Confluence(10) Choppy(11) BotVer(12)
  var wins = 0, totalRR = 0;

  data.forEach(function(row) {
    var outcome = String(row[3]).toUpperCase();
    var rr      = Number(row[8]) || 0;
    if (outcome === "CLOSED") { wins++; }
    totalRR += rr;
  });

  var total   = data.length;
  var winRate = total > 0 ? (wins / total * 100).toFixed(1) : "0.0";
  var avgRR   = total > 0 ? (totalRR / total).toFixed(2)    : "0.0";

  // Get equity from EOD sheet
  var eodSheet = ss.getSheetByName(SHEETS.koi_eod);
  var equity = 0, totalPnl = 0;
  if (eodSheet && eodSheet.getLastRow() > 1) {
    var lastEodRow = eodSheet.getLastRow();
    equity   = Number(eodSheet.getRange(lastEodRow, 6).getValue()) || 0;
    totalPnl = Number(eodSheet.getRange(lastEodRow, 11).getValue()) || 0;
  }

  return { totalTrades: total, wins: wins, winRate: winRate,
           avgRR: avgRR, totalPnl: totalPnl, equity: equity };
}

// ── KoiScale formatting ──────────────────────────────────────
function formatKoiTradesSheet(sheet) {
  var last = sheet.getLastRow();
  if (last < 2) { return; }
  var outcome = String(sheet.getRange(last, 4).getValue()).toUpperCase();
  var bg = "#ffffff";
  if      (outcome === "CLOSED")    { bg = "#b7e1cd"; }  // green
  else if (outcome === "STOPPED")   { bg = "#f4c7c3"; }  // red
  else if (outcome === "STOPPED_BE"){ bg = "#f8f9fa"; }  // grey
  else if (outcome === "EOD")       { bg = "#fce8b2"; }  // yellow
  sheet.getRange(last, 1, 1, sheet.getLastColumn()).setBackground(bg);
  [1, 2, 3, 4].forEach(function(c) { sheet.autoResizeColumn(c); });
}

function formatKoiEODSheet(sheet) {
  var last = sheet.getLastRow();
  if (last < 2) { return; }
  var pnl     = Number(sheet.getRange(last, 5).getValue());
  var pnlCell = sheet.getRange(last, 5);
  if      (pnl > 0) { pnlCell.setBackground("#b7e1cd"); }
  else if (pnl < 0) { pnlCell.setBackground("#f4c7c3"); }
  else              { pnlCell.setBackground("#f8f9fa"); }
  [1, 2, 3].forEach(function(c) { sheet.autoResizeColumn(c); });
}

// ================================================================
//  GREENCLAW HANDLERS (unchanged from original)
// ================================================================

// ── Trade handler ────────────────────────────────────────────
function handleTrade(d) {
  var sheet = getOrCreateSheet(SHEETS.trades, [
    "Date", "Time", "Symbol", "Qty", "Entry $", "Stop $", "Target $",
    "Risk/sh", "Total Risk $", "R:R", "Target Type", "Size",
    "Float", "Catalyst", "VWAP", "HOD", "Pole Ht", "B-Mode",
    "Equity Before", "Trade #", "Order ID", "Result $", "Win/Loss"
  ]);

  sheet.appendRow([
    d.date         || "",
    formatTime(d.timestamp),
    d.symbol       || "",
    d.qty          || "",
    d.entry        || "",
    d.stop         || "",
    d.target       || "",
    d.risk         || "",
    d.total_risk   || "",
    d.rr           || "",
    d.target_type  || "",
    d.size_label   || "",
    d.float        || "",
    String(d.catalyst || "").substring(0, 60),
    d.vwap         || "",
    d.hod          || "",
    d.pole_height  || "",
    d.b_mode ? "Yes" : "No",
    d.equity       || "",
    d.trade_num    || "",
    d.order_id     || "",
    "",
    ""
  ]);

  formatTradesSheet(sheet);
}

// ── EOD handler ──────────────────────────────────────────────
function handleEOD(d) {
  var sheet = getOrCreateSheet(SHEETS.eod, [
    "Date", "Day", "Outcome", "Trades", "P&L $", "Equity $",
    "Prev Equity $", "Candidates", "B-Mode", "Running P&L $",
    "Win Rate %", "Avg R:R"
  ]);

  var lastRow    = sheet.getLastRow();
  var runningPnl = 0;
  if (lastRow > 1) {
    var prevRunning = sheet.getRange(lastRow, 10).getValue();
    runningPnl = (Number(prevRunning) || 0) + (Number(d.pnl) || 0);
  } else {
    runningPnl = Number(d.pnl) || 0;
  }

  var stats = calcStats();

  sheet.appendRow([
    d.date        || "",
    d.day_name    || "",
    d.outcome     || "",
    d.trades      || 0,
    d.pnl         || 0,
    d.equity      || 0,
    d.last_equity || 0,
    d.candidates  || 0,
    d.b_mode ? "Yes" : "No",
    runningPnl,
    stats.winRate,
    stats.avgRR
  ]);

  formatEODSheet(sheet);
}

// ── Scan handler ─────────────────────────────────────────────
function handleScan(d) {
  var sheet = getOrCreateSheet(SHEETS.scan, [
    "Date", "Time", "Candidates", "B-Mode", "Equity $", "Symbols Found"
  ]);

  var watchlist = d.watchlist || [];
  var symbols   = watchlist.map(function(w) { return w.symbol; }).join(", ") || "None";

  sheet.appendRow([
    d.date       || "",
    formatTime(d.timestamp),
    d.candidates || 0,
    d.b_mode ? "Yes" : "No",
    d.equity     || 0,
    symbols
  ]);
}

// ── Ross Insights handler ─────────────────────────────────────
function handleRossInsight(d) {
  var sheet = getOrCreateSheet(SHEETS.ross, [
    "Date", "Day", "Video Title", "Video URL",
    "Result", "Market Condition",
    "Stocks Traded", "Setups Used",
    "What's Working", "What's NOT Working",
    "GreenClaw Relevance", "Next Session Outlook",
    "Quotable Insight", "Full Analysis", "Transcript Length"
  ]);

  var analysis = d.analysis || "";

  function extractSection(label, maxLen) {
    maxLen = maxLen || 2000;
    var marker = "**" + label + ":**";
    var start  = analysis.indexOf(marker);
    if (start === -1) { return ""; }
    start = start + marker.length;

    var searchFrom = start;
    var end = analysis.length;
    while (searchFrom < analysis.length) {
      var nextBold = analysis.indexOf("\n**", searchFrom);
      if (nextBold === -1) { break; }
      var lineEnd = analysis.indexOf("\n", nextBold + 1);
      if (lineEnd === -1) { lineEnd = analysis.length; }
      var line = analysis.substring(nextBold + 1, lineEnd);
      if (line.indexOf(":**") !== -1) {
        end = nextBold;
        break;
      }
      searchFrom = nextBold + 1;
    }
    return analysis.substring(start, end).trim().substring(0, maxLen);
  }

  function extractResult() {
    var marker = "**Result:**";
    var start  = analysis.indexOf(marker);
    if (start === -1) { return ""; }
    start = start + marker.length;
    var end = analysis.indexOf("\n", start);
    if (end === -1) { end = Math.min(start + 120, analysis.length); }
    return analysis.substring(start, end).trim();
  }

  sheet.appendRow([
    d.date                  || "",
    d.day_name              || "",
    d.video_title           || "",
    d.video_url             || "",
    extractResult(),
    extractSection("Market Condition"),
    extractSection("Stocks Traded"),
    extractSection("Setup Types Used Today"),
    extractSection("What's Working Right Now"),
    extractSection("What's NOT Working"),
    extractSection("GreenClaw Relevance"),
    extractSection("Market Condition for Next Session"),
    extractSection("Quotable Insight"),
    analysis.substring(0, 50000),
    d.transcript_length     || 0
  ]);

  var last = sheet.getLastRow();
  sheet.getRange(last, 1, 1, sheet.getLastColumn())
    .setBackground(last % 2 === 0 ? "#f8f9fa" : "#ffffff");
  sheet.getRange(last, 14).setWrap(true);
  [1, 2, 3, 5, 6].forEach(function(c) { sheet.autoResizeColumn(c); });
}

// ================================================================
//  GREENCLAW SUMMARY + STATS (unchanged)
// ================================================================

function updateSummary() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEETS.summary);
  if (!sheet) {
    sheet = ss.insertSheet(SHEETS.summary);
    ss.setActiveSheet(sheet);
    ss.moveActiveSheet(1);
  }
  sheet.clearContents();

  var stats     = calcStats();
  var eodSheet  = ss.getSheetByName(SHEETS.eod);
  var greenDays = eodSheet ? countGreenDays(eodSheet) : 0;
  var totalDays = eodSheet ? Math.max(eodSheet.getLastRow() - 1, 0) : 0;
  var greenPct  = totalDays > 0
    ? (greenDays / totalDays * 100).toFixed(1) + "%" : "--";

  var readyStatus = (stats.totalTrades >= 100 && parseFloat(stats.winRate) >= 65)
    ? "READY FOR LIVE TRADING" : "Paper phase in progress";

  var data = [
    ["GreenClaw Performance Summary", ""],
    ["Last updated", new Date().toLocaleString()],
    ["", ""],
    ["-- Paper Trading Stats --", ""],
    ["Total trades",           stats.totalTrades],
    ["Wins",                   stats.wins],
    ["Win rate",               stats.winRate + "%"],
    ["Avg R:R",                stats.avgRR],
    ["Total P&L",              "$" + stats.totalPnl.toFixed(2)],
    ["Current equity",         "$" + stats.equity.toFixed(2)],
    ["", ""],
    ["-- Daily Stats --", ""],
    ["Trading days",           totalDays],
    ["Green days",             greenDays],
    ["Red days",               totalDays - greenDays],
    ["Green day rate",         greenPct],
    ["", ""],
    ["-- Progress to Live Trading --", ""],
    ["Target trades (paper)",  100],
    ["Completed",              stats.totalTrades],
    ["Remaining",              Math.max(0, 100 - stats.totalTrades)],
    ["Min win rate needed",    "65%"],
    ["Current win rate",       stats.winRate + "%"],
    ["Status",                 readyStatus]
  ];

  sheet.getRange(1, 1, data.length, 2).setValues(data);

  sheet.getRange(1, 1, 1, 2).setFontWeight("bold").setFontSize(14);
  [4, 12, 18].forEach(function(r) {
    sheet.getRange(r, 1).setFontWeight("bold");
  });
  sheet.setColumnWidth(1, 260);
  sheet.setColumnWidth(2, 180);

  var wr     = parseFloat(stats.winRate);
  var wrCell = sheet.getRange(7, 2);
  if      (wr >= 65) { wrCell.setBackground("#b7e1cd"); }
  else if (wr >= 50) { wrCell.setBackground("#fce8b2"); }
  else               { wrCell.setBackground("#f4c7c3"); }
}

function calcStats() {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(SHEETS.trades);
  if (!sheet || sheet.getLastRow() < 2) {
    return { totalTrades: 0, wins: 0, winRate: "0.0",
             avgRR: "0.0", totalPnl: 0, equity: 0 };
  }

  var data = sheet.getRange(
    2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()
  ).getValues();

  var wins = 0, totalRR = 0, totalPnl = 0, equity = 0;

  data.forEach(function(row) {
    var result  = row[21];
    var winLoss = row[22];
    var rr      = Number(row[9])  || 0;
    var eq      = Number(row[18]) || 0;

    if (winLoss === "W") { wins++; }
    if (result !== "" && result !== null) {
      totalPnl += Number(result) || 0;
    }
    totalRR += rr;
    if (eq > equity) { equity = eq; }
  });

  var total   = data.length;
  var winRate = total > 0 ? (wins / total * 100).toFixed(1) : "0.0";
  var avgRR   = total > 0 ? (totalRR / total).toFixed(2)    : "0.0";

  return { totalTrades: total, wins: wins, winRate: winRate,
           avgRR: avgRR, totalPnl: totalPnl, equity: equity };
}

// ================================================================
//  SHARED HELPERS
// ================================================================

function countGreenDays(sheet) {
  if (sheet.getLastRow() < 2) { return 0; }
  var pnls  = sheet.getRange(2, 5, sheet.getLastRow() - 1, 1).getValues();
  var count = 0;
  pnls.forEach(function(r) {
    if (Number(r[0]) > 0) { count++; }
  });
  return count;
}

function getOrCreateSheet(name, headers) {
  var ss    = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
    sheet.appendRow(headers);
    sheet.getRange(1, 1, 1, headers.length)
      .setFontWeight("bold")
      .setBackground("#1a73e8")
      .setFontColor("#ffffff");
    sheet.setFrozenRows(1);
  }
  return sheet;
}

function formatTime(ts) {
  if (!ts) { return ""; }
  try   { return new Date(ts).toLocaleTimeString(); }
  catch (e) { return String(ts); }
}

function formatDate(ts) {
  if (!ts) { return ""; }
  try   { return new Date(ts).toLocaleDateString(); }
  catch (e) { return String(ts); }
}

function formatTradesSheet(sheet) {
  var last = sheet.getLastRow();
  if (last < 2) { return; }
  sheet.getRange(last, 1, 1, sheet.getLastColumn())
    .setBackground(last % 2 === 0 ? "#f8f9fa" : "#ffffff");
  [1, 2, 3, 11, 12].forEach(function(c) { sheet.autoResizeColumn(c); });
}

function formatEODSheet(sheet) {
  var last    = sheet.getLastRow();
  if (last < 2) { return; }
  var pnl     = Number(sheet.getRange(last, 5).getValue());
  var pnlCell = sheet.getRange(last, 5);
  if      (pnl > 0) { pnlCell.setBackground("#b7e1cd"); }
  else if (pnl < 0) { pnlCell.setBackground("#f4c7c3"); }
  else              { pnlCell.setBackground("#f8f9fa"); }
  [1, 2, 3].forEach(function(c) { sheet.autoResizeColumn(c); });
}
