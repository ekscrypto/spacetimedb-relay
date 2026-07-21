(function () {
  "use strict";

  // Each .code-switch wraps one tab strip + its panels.
  document.querySelectorAll(".code-switch").forEach(function (root) {
    var tabs = root.querySelector(".lang-tabs");
    if (!tabs) return;
    var buttons = tabs.querySelectorAll("button[data-lang]");
    var panels = root.querySelectorAll(".code-panel");
    buttons.forEach(function (btn) {
      btn.addEventListener("click", function () {
        var lang = btn.getAttribute("data-lang");
        buttons.forEach(function (b) {
          b.classList.toggle("active", b === btn);
        });
        panels.forEach(function (panel) {
          panel.classList.toggle(
            "active",
            panel.getAttribute("data-lang") === lang
          );
        });
      });
    });
  });

  document.querySelectorAll(".code-panel").forEach(function (panel) {
    var pre = panel.querySelector("pre");
    if (!pre) return;
    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "copy";
    btn.textContent = "Copy";
    btn.addEventListener("click", function () {
      var text = pre.innerText || pre.textContent || "";
      function ok() {
        btn.textContent = "Copied";
        btn.classList.add("ok");
        setTimeout(function () {
          btn.textContent = "Copy";
          btn.classList.remove("ok");
        }, 1200);
      }
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(ok).catch(function () {
          fallbackCopy(text, ok);
        });
      } else {
        fallbackCopy(text, ok);
      }
    });
    panel.appendChild(btn);
  });

  function fallbackCopy(text, ok) {
    var ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand("copy");
      ok();
    } catch (_) { /* ignore */ }
    document.body.removeChild(ta);
  }

  var file = (location.pathname.split("/").pop() || "index.html").toLowerCase();
  if (!file || file === "tutorial") file = "index.html";
  document.querySelectorAll(".sidebar nav a").forEach(function (a) {
    var href = (a.getAttribute("href") || "").split("/").pop().toLowerCase();
    if (href === file) a.classList.add("active");
  });
})();
