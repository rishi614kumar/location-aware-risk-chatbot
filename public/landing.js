// Landing Page Script with Reopen Control
// Creates an overlay landing/intro screen using Columbia branding.
// Includes a persistent "Show Intro" button to bring the overlay back.

(function () {
  const STORAGE_KEY = "cuLandingDismissed";
  const LANDING_ID = "cu-landing";
  const BUTTON_ID = "cu-landing-return-btn";
  const BUTTON_CONTAINER_ID = "cu-landing-return-container";
  let landingRoot = null;
  let observer = null;

  // Don't run on shared threads
  if (window.location.pathname.startsWith("/share")) return;

  // -------------------------------
  // Landing overlay
  // -------------------------------
  function buildLanding() {
    const root = document.createElement("div");
    root.id = LANDING_ID;
    root.setAttribute("role", "dialog");
    root.setAttribute("aria-modal", "true");
    root.style.position = "fixed";
    root.style.inset = "0";
    root.style.zIndex = "9999";
    root.style.display = "flex";
    root.style.flexDirection = "column";
    root.style.alignItems = "center";
    root.style.justifyContent = "center";
    root.style.padding = "2.5rem 1.5rem";
    root.style.background =
      "radial-gradient(1200px 900px at 15% -10%, var(--cu-blue-50) 0%, transparent 60%)," +
      "radial-gradient(1400px 1100px at 110% 110%, var(--cu-blue) 0%, transparent 70%)," +
      "linear-gradient(135deg, var(--cu-blue) 0%, var(--cu-blue-50) 100%)";
    root.style.backdropFilter = "blur(4px)";
    root.style.animation = "fadeIn 480ms var(--easing)";

    const card = document.createElement("div");
    card.style.width = "min(840px, 100%)";
    card.style.background = "var(--glass-strong)";
    card.style.border = "1px solid var(--divider)";
    card.style.boxShadow = "var(--shadow-lg)";
    card.style.borderRadius = "28px";
    card.style.padding = "3rem clamp(1.4rem, 4vw, 3.2rem) 2.6rem";
    card.style.display = "flex";
    card.style.flexDirection = "column";
    card.style.alignItems = "flex-start";
    card.style.gap = "1.75rem";
    card.style.position = "relative";
    card.style.overflow = "hidden";

    // Decorative crown
    const deco = document.createElement("div");
    deco.textContent = "♕";
    deco.style.position = "absolute";
    deco.style.fontSize = "280px";
    deco.style.lineHeight = "1";
    deco.style.opacity = ".05";
    deco.style.right = "-40px";
    deco.style.top = "-60px";
    deco.style.pointerEvents = "none";
    deco.style.fontFamily = "var(--font-serif)";
    card.appendChild(deco);

    // Logo
    const logo = document.createElement("img");
    logo.src = "/public/columbia_logo.png";
    logo.alt = "Columbia University Crown Logo";
    logo.style.width = "86px";
    logo.style.height = "86px";
    logo.style.objectFit = "contain";
    logo.style.filter = "drop-shadow(0 6px 14px rgba(0,51,102,.25))";
    logo.style.borderRadius = "50%";

    // Title
    const title = document.createElement("h1");
    title.textContent = "NYC Location Aware Risk Chatbot";
    title.style.fontFamily = "var(--font-serif)";
    title.style.fontSize = "clamp(1.9rem, 5.2vw, 3.1rem)";
    title.style.margin = "0";
    title.style.color = "var(--cu-navy)";
    title.style.textShadow = "0 2px 6px rgba(0,51,102,.12)";

    // Tagline
    const tagline = document.createElement("p");
    tagline.textContent =
      "No more digging through spreadsheets or maps—this system converts complex geospatial data into simple, conversational answers tailored to each project site.";
    tagline.style.fontSize = "clamp(1rem, 1.15rem, 1.2rem)";
    tagline.style.lineHeight = "1.55";
    tagline.style.margin = "0";
    tagline.style.maxWidth = "60ch";
    tagline.style.color = "var(--text)";

    // Feature list
    const features = [
      "Location-aware risk analysis from any NYC address",
      "Natural-language querying powered by GenAI",
      "Automatically unifies scattered geospatial datasets",
      "Clear, actionable risk summaries in seconds",
    ];
    const ul = document.createElement("ul");
    ul.style.listStyle = "none";
    ul.style.margin = "0";
    ul.style.padding = "0";
    ul.style.display = "grid";
    ul.style.gap = ".65rem";
    ul.style.fontSize = ".95rem";
    features.forEach((f) => {
      const li = document.createElement("li");
      li.innerHTML =
        `<span style="display:inline-flex;align-items:center;gap:.55rem">` +
        `<span style="background:var(--cu-blue);color:var(--cu-navy);width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;border-radius:999px;font-size:.75rem;font-weight:700;box-shadow:0 4px 10px rgba(0,51,102,.25)">✓</span>` +
        `${f}</span>`;
      ul.appendChild(li);
    });

    // Start button
    const startBtn = document.createElement("button");
    startBtn.type = "button";
    startBtn.textContent = "Enter Chat";
    startBtn.setAttribute(
      "aria-label",
      "Enter Chat and begin conversation",
    );
    startBtn.style.marginTop = ".35rem";
    startBtn.style.padding = "0 1.25rem";
    startBtn.style.height = "52px";
    startBtn.style.fontSize = "1.05rem";
    startBtn.style.borderRadius = "18px";

    // Secondary links
    const links = document.createElement("div");
    links.style.display = "flex";
    links.style.flexWrap = "wrap";
    links.style.gap = "1rem";
    links.style.marginTop = ".25rem";

    function makeLink(text, href) {
      const a = document.createElement("a");
      a.textContent = text;
      a.href = href;
      a.target = "_blank";
      a.rel = "noopener noreferrer";
      a.style.textDecoration = "none";
      a.style.fontSize = ".85rem";
      a.style.fontWeight = "600";
      a.style.padding = ".55rem .85rem";
      a.style.border = "1px solid var(--divider)";
      a.style.borderRadius = "12px";
      a.style.background = "var(--surface)";
      a.style.color = "var(--cu-deep)";
      a.style.boxShadow = "var(--shadow-xs)";
      a.addEventListener("mouseenter", () => {
        a.style.background =
          "color-mix(in oklab, var(--cu-blue-50) 55%, transparent)";
      });
      a.addEventListener("mouseleave", () => {
        a.style.background = "var(--surface)";
      });
      return a;
    }

    links.appendChild(
      makeLink(
        "Repository",
        "https://github.com/rishi614kumar/location-aware-risk-chatbot",
      ),
    );
    links.appendChild(
      makeLink("NYC Open Data", "https://opendata.cityofnewyork.us"),
    );

    // Disclaimer
    const disclaimer = document.createElement("small");
    disclaimer.textContent =
      "Made in collaboration with Town+Gown: NYC @ DDC. LLM output may contain inaccuracies.";
    disclaimer.style.opacity = ".75";
    disclaimer.style.fontSize = ".7rem";
    disclaimer.style.marginTop = ".5rem";

    // Close logic
    startBtn.addEventListener("click", () => hideLanding(true));
    root.addEventListener("keydown", (e) => {
      if (e.key === "Escape") hideLanding(true);
      if (e.key === "Enter" && !e.shiftKey) hideLanding(true);
    });

    // Layout
    card.appendChild(logo);
    card.appendChild(title);
    card.appendChild(tagline);
    card.appendChild(ul);
    card.appendChild(startBtn);
    card.appendChild(links);
    card.appendChild(disclaimer);
    root.appendChild(card);

    // Trap focus on overlay
    const focusable = () =>
      Array.from(
        root.querySelectorAll(
          'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])',
        ),
      );
    root.addEventListener("keydown", (e) => {
      if (e.key === "Tab") {
        const items = focusable();
        if (!items.length) return;
        const idx = items.indexOf(document.activeElement);
        if (e.shiftKey) {
          if (idx <= 0) {
            items[items.length - 1].focus();
            e.preventDefault();
          }
        } else {
          if (idx === items.length - 1) {
            items[0].focus();
            e.preventDefault();
          }
        }
      }
    });

    landingRoot = root;
    document.body.appendChild(root);
    setTimeout(() => startBtn.focus(), 50);
    return root;
  }

  // Tiny fade-in keyframe
  function ensureKeyframe() {
    if (document.getElementById("cu-landing-anim")) return;
    const style = document.createElement("style");
    style.id = "cu-landing-anim";
    style.textContent =
      "@keyframes fadeIn { from { opacity:0; transform:translateY(8px) scale(.985);} to { opacity:1; transform:translateY(0) scale(1);} }";
    document.head.appendChild(style);
  }

  // -------------------------------
  // Hide Readme + theme toggle
  // -------------------------------
  function hideNativeHeaderButtons() {
    const candidates = document.querySelectorAll(
      "button, a, [role='button'], [data-testid]",
    );

    candidates.forEach((el) => {
      const label = (el.getAttribute("aria-label") || "").toLowerCase();
      const testid = (el.getAttribute("data-testid") || "").toLowerCase();
      const text = (el.textContent || "").trim().toLowerCase();

      // Kill anything that looks like "Readme"
      if (
        text === "readme" ||
        label.includes("readme") ||
        testid.includes("readme")
      ) {
        el.style.display = "none";
        return;
      }

      // Kill any theme / color-mode toggle
      if (
        label.includes("theme") ||
        label.includes("dark mode") ||
        label.includes("light mode") ||
        testid.includes("theme") ||
        testid.includes("color-mode")
      ) {
        el.style.display = "none";
        return;
      }
    });
  }

  // -------------------------------
  // Show / hide landing
  // -------------------------------
  function hideLanding(storeDismissal = true) {
    if (!landingRoot) return;
    landingRoot.style.opacity = "0";
    landingRoot.style.pointerEvents = "none";
    const root = landingRoot;
    setTimeout(() => {
      if (root.parentElement) root.remove();
      if (landingRoot === root) landingRoot = null;
      ensureReturnButton();
    }, 260);
    if (storeDismissal) sessionStorage.setItem(STORAGE_KEY, "1");
    const composer = document.querySelector(
      ".cl-composer textarea, .cl-composer input, textarea.cl-input",
    );
    if (composer) composer.focus();
  }

  function showLanding(resetFlag = false) {
    if (resetFlag) sessionStorage.removeItem(STORAGE_KEY);
    removeReturnButton();
    if (landingRoot) {
      landingRoot.remove();
      landingRoot = null;
    }
    ensureKeyframe();
    const root = buildLanding();
    requestAnimationFrame(() => {
      if (!root) return;
      root.style.opacity = "1";
      root.style.pointerEvents = "auto";
    });
  }

  // -------------------------------
  // Homepage button in header / floating
  // -------------------------------
  function ensureReturnButton() {
    const header = document.querySelector(".cl-header");

    // Always hide native controls whenever we touch header
    hideNativeHeaderButtons();

    if (document.getElementById(BUTTON_ID)) return;

    const btn = document.createElement("button");
    btn.id = BUTTON_ID;
    btn.type = "button";
    btn.textContent = "Homepage";
    btn.setAttribute(
      "aria-label",
      "Show the introductory landing overlay again",
    );
    btn.addEventListener("click", () => showLanding(true));

    if (header) {
      btn.classList.add("cl-button-ghost", "cu-landing-return");
      header.appendChild(btn);
      return;
    }

    // Fallback: floating bottom-right
    btn.classList.add("cu-landing-return-floating");
    const existingContainer = document.getElementById(BUTTON_CONTAINER_ID);
    const container = existingContainer || document.createElement("div");
    if (!existingContainer) {
      container.id = BUTTON_CONTAINER_ID;
      document.body.appendChild(container);
    }
    container.appendChild(btn);
  }

  function removeReturnButton() {
    const btn = document.getElementById(BUTTON_ID);
    if (!btn) return;
    const parent = btn.parentElement;
    btn.remove();
    if (parent && parent.id === BUTTON_CONTAINER_ID) parent.remove();
  }

  // -------------------------------
  // MutationObserver: keep header clean
  // -------------------------------
  function setupObservers() {
    if (observer) return;
    observer = new MutationObserver(() => {
      ensureReturnButton();
      hideNativeHeaderButtons();
    });
    observer.observe(document.body, { childList: true, subtree: true });
    ensureReturnButton();
    hideNativeHeaderButtons();
  }

  // -------------------------------
  // Init
  // -------------------------------
  function init() {
    ensureKeyframe();
    hideNativeHeaderButtons();
    if (!sessionStorage.getItem(STORAGE_KEY)) {
      showLanding();
    } else {
      ensureReturnButton();
    }
    setupObservers();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  // Tiny debug hook, if you ever need it:
  window.CULanding = {
    show: () => showLanding(true),
    hide: () => hideLanding(false),
    reset: () => {
      sessionStorage.removeItem(STORAGE_KEY);
      showLanding();
    },
    hideHeaderButtons: hideNativeHeaderButtons,
  };
})();
