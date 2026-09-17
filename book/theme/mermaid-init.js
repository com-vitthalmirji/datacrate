(function () {
  var script = document.createElement("script");
  script.src = "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js";
  script.onload = function () {
    mermaid.initialize({ startOnLoad: false, theme: "neutral" });
    document.querySelectorAll("pre code.language-mermaid").forEach(function (code, i) {
      var div = document.createElement("div");
      div.className = "mermaid";
      div.id = "mermaid-diagram-" + i;
      div.textContent = code.textContent;
      code.parentElement.replaceWith(div);
    });
    mermaid.run().then(function () {
      document.querySelectorAll(".mermaid").forEach(function (div) {
        div.addEventListener("click", function () {
          var svg = div.querySelector("svg");
          if (!svg) return;
          var overlay = document.createElement("div");
          overlay.className = "mermaid-zoom-overlay";
          overlay.appendChild(svg.cloneNode(true));
          overlay.addEventListener("click", function () {
            overlay.remove();
          });
          document.addEventListener("keydown", function onKey(e) {
            if (e.key === "Escape") {
              overlay.remove();
              document.removeEventListener("keydown", onKey);
            }
          });
          document.body.appendChild(overlay);
        });
      });
    });
  };
  document.head.appendChild(script);
})();
