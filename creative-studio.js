/**
 * Brand Sale Creative Studio Engine
 * Multi-Ratio Generator (1:1, 9:16, 16:9, 4:3)
 */

(function () {
  "use strict";

  // --- Campaign Preset Definitions ---
  const CAMPAIGN_PRESETS = {
    testbook_monsoon: {
      saleTag: "⚡ MONSOON MEGA SALE",
      discount: "FLAT 70% OFF",
      headline: "On All SSC, Railway & State Govt SuperCoachings",
      subline: "Free Pass Pro + 50,000+ Mock Tests Included",
      originalPrice: "₹4,999",
      offerPrice: "₹999",
      coupon: "USE CODE: CRACKIT",
      cta: "ENROLL NOW →",
      disclaimer: "*Valid till midnight only. T&C Apply.",
      brandPreset: "testbook",
      theme: "theme-modern-electric",
      badgeStyle: "badge-pill",
      hero: "faculty",
    },
    exam_pass: {
      saleTag: "🎯 SELECTION KA UTSAV",
      discount: "UPTO 85% OFF",
      headline: "Yearly Testbook Pass Pro at Lowest Price Ever",
      subline: "Unlimited Access to 750+ Exams & 75,000+ Mock Tests",
      originalPrice: "₹1,999",
      offerPrice: "₹399",
      coupon: "USE CODE: PASSPRO",
      cta: "GET PASS PRO →",
      disclaimer: "*Guaranteed lowest price of the season. T&C Apply.",
      brandPreset: "testbook",
      theme: "theme-modern-electric",
      badgeStyle: "badge-ribbon",
      hero: "pass",
    },
    supercoaching: {
      saleTag: "🏆 GRAND VICTORY SALE",
      discount: "FLAT 65% OFF",
      headline: "India's Super Teachers for UPSC & State PSC",
      subline: "Complete Live Coaching + Daily Notes + Mentorship",
      originalPrice: "₹14,999",
      offerPrice: "₹4,499",
      coupon: "USE CODE: SUPER65",
      cta: "START PREPARING →",
      disclaimer: "*Limited seats available for live batch.",
      brandPreset: "supercoaching",
      theme: "theme-festive-gold",
      badgeStyle: "badge-stamp",
      hero: "trophy",
    },
    flash_midnight: {
      saleTag: "🔥 MIDNIGHT FLASH DEAL",
      discount: "FLAT 85% OFF",
      headline: "3-Hour Flash Sale on All Defence & Police Courses",
      subline: "Only 100 Coupon Codes Active. Hurry!",
      originalPrice: "₹3,499",
      offerPrice: "₹499",
      coupon: "USE CODE: FLASH85",
      cta: "CLAIM OFFER NOW →",
      disclaimer: "*Offer expires sharp at 12:00 AM.",
      brandPreset: "cyberdark",
      theme: "theme-neon-dark",
      badgeStyle: "badge-pill",
      hero: "books",
    },
  };

  // --- Brand Kit Presets ---
  const BRAND_PRESETS = {
    testbook: {
      name: "testbook",
      primary: "#00A8E8",
      secondary: "#001B3A",
      accent: "#FFB703",
      bg: "#071228",
      font: "'Plus Jakarta Sans', sans-serif",
    },
    supercoaching: {
      name: "SuperCoaching",
      primary: "#FF9F1C",
      secondary: "#2B0938",
      accent: "#2EC4B6",
      bg: "#16041F",
      font: "'Outfit', sans-serif",
    },
    cyberdark: {
      name: "testbook PRO",
      primary: "#06D6A0",
      secondary: "#0D1B2A",
      accent: "#FF007F",
      bg: "#050811",
      font: "'Space Grotesk', sans-serif",
    },
    royalpurple: {
      name: "testbook ELITE",
      primary: "#8338EC",
      secondary: "#1A0B2E",
      accent: "#FFBE0B",
      bg: "#0D0417",
      font: "'Plus Jakarta Sans', sans-serif",
    },
  };

  // --- Sample Hero Icons / SVGs ---
  const SAMPLE_HERO_EMOJIS = {
    faculty: "👨‍🏫",
    trophy: "🏆",
    pass: "🎟️",
    books: "📚",
  };

  // --- State ---
  const state = {
    saleTag: "⚡ MONSOON MEGA SALE",
    discount: "FLAT 70% OFF",
    headline: "On All SSC, Railway & State Govt Courses",
    subline: "Free Pass Pro + 50,000+ Mock Tests Included",
    originalPrice: "₹4,999",
    offerPrice: "₹999",
    coupon: "USE CODE: CRACKIT",
    cta: "ENROLL NOW →",
    disclaimer: "*Valid till midnight only. T&C Apply.",
    brandName: "testbook",
    brandLogoUrl: "/assets/testbook-logo.png",
    customHeroUrl: "",
    selectedSampleHero: "faculty",
    heroScale: 1,
    heroPosY: 0,
    heroGlow: true,
    badgeGlow: true,
    bgGrid: true,
    sparkles: true,
    showPriceBox: true,
    badgeStyle: "badge-pill",
    theme: "theme-modern-electric",
    font: "'Plus Jakarta Sans', sans-serif",
    colors: {
      primary: "#00A8E8",
      secondary: "#001B3A",
      accent: "#FFB703",
      bg: "#071228",
    },
    zoom: 0.8,
  };

  // --- DOM References ---
  const elements = {
    // Inputs
    inputSaleTag: document.getElementById("inputSaleTag"),
    inputDiscount: document.getElementById("inputDiscount"),
    inputHeadline: document.getElementById("inputHeadline"),
    inputSubline: document.getElementById("inputSubline"),
    inputOriginalPrice: document.getElementById("inputOriginalPrice"),
    inputOfferPrice: document.getElementById("inputOfferPrice"),
    inputCouponCode: document.getElementById("inputCouponCode"),
    inputCtaText: document.getElementById("inputCtaText"),
    inputDisclaimer: document.getElementById("inputDisclaimer"),
    inputBrandName: document.getElementById("inputBrandName"),
    inputBrandLogo: document.getElementById("inputBrandLogo"),
    btnResetLogo: document.getElementById("btnResetLogo"),
    logoPreviewImg: document.getElementById("logoPreviewImg"),
    inputHeroImage: document.getElementById("inputHeroImage"),
    sliderHeroScale: document.getElementById("sliderHeroScale"),
    sliderHeroPosY: document.getElementById("sliderHeroPosY"),
    valHeroScale: document.getElementById("valHeroScale"),
    valHeroPosY: document.getElementById("valHeroPosY"),
    chkHeroGlow: document.getElementById("chkHeroGlow"),
    chkShowBadgeGlow: document.getElementById("chkShowBadgeGlow"),
    chkShowBgGrid: document.getElementById("chkShowBgGrid"),
    chkShowSparkles: document.getElementById("chkShowSparkles"),
    chkShowPriceBox: document.getElementById("chkShowPriceBox"),
    badgeStyleSelect: document.getElementById("badgeStyleSelect"),
    fontSelect: document.getElementById("fontSelect"),
    campaignPresetSelect: document.getElementById("campaignPresetSelect"),

    // Colors
    colorPrimary: document.getElementById("colorPrimary"),
    colorSecondary: document.getElementById("colorSecondary"),
    colorAccent: document.getElementById("colorAccent"),
    colorBg: document.getElementById("colorBg"),
    hexPrimary: document.getElementById("hexPrimary"),
    hexSecondary: document.getElementById("hexSecondary"),
    hexAccent: document.getElementById("hexAccent"),
    hexBg: document.getElementById("hexBg"),

    // Zoom & Export
    btnZoomIn: document.getElementById("btnZoomIn"),
    btnZoomOut: document.getElementById("btnZoomOut"),
    btnResetZoom: document.getElementById("btnResetZoom"),
    zoomLevelDisplay: document.getElementById("zoomLevelDisplay"),
    btnExportAll: document.getElementById("btnExportAll"),
    toast: document.getElementById("studioToast"),

    // Artboards
    artboards: document.querySelectorAll(".creative-artboard"),
  };

  // --- Initialization ---
  function init() {
    bindTabs();
    bindInputs();
    bindColors();
    bindPresets();
    bindHeroControls();
    bindZoomControls();
    bindExportActions();
    updateUI();
  }

  // --- Tab Switcher ---
  function bindTabs() {
    const tabButtons = document.querySelectorAll(".tab-btn");
    tabButtons.forEach((btn) => {
      btn.addEventListener("click", () => {
        tabButtons.forEach((b) => b.classList.remove("active"));
        document.querySelectorAll(".tab-panel").forEach((p) => p.classList.remove("active"));
        btn.classList.add("active");
        const target = document.getElementById(`tab-${btn.dataset.tab}`);
        if (target) target.classList.add("active");
      });
    });
  }

  // --- Bind Inputs to State ---
  function bindInputs() {
    const textBindings = [
      { el: elements.inputSaleTag, key: "saleTag" },
      { el: elements.inputDiscount, key: "discount" },
      { el: elements.inputHeadline, key: "headline" },
      { el: elements.inputSubline, key: "subline" },
      { el: elements.inputOriginalPrice, key: "originalPrice" },
      { el: elements.inputOfferPrice, key: "offerPrice" },
      { el: elements.inputCouponCode, key: "coupon" },
      { el: elements.inputCtaText, key: "cta" },
      { el: elements.inputDisclaimer, key: "disclaimer" },
      { el: elements.inputBrandName, key: "brandName" },
    ];

    textBindings.forEach(({ el, key }) => {
      if (!el) return;
      el.addEventListener("input", (e) => {
        state[key] = e.target.value;
        renderContent();
      });
    });

    // Quick chips
    document.querySelectorAll(".chip").forEach((chip) => {
      chip.addEventListener("click", () => {
        const targetId = chip.dataset.target;
        const val = chip.dataset.val;
        const input = document.getElementById(targetId);
        if (input) {
          input.value = val;
          input.dispatchEvent(new Event("input"));
        }
      });
    });

    // Font select
    if (elements.fontSelect) {
      elements.fontSelect.addEventListener("change", (e) => {
        state.font = e.target.value;
        document.documentElement.style.setProperty("--brand-font", state.font);
      });
    }

    // Badge style select
    if (elements.badgeStyleSelect) {
      elements.badgeStyleSelect.addEventListener("change", (e) => {
        state.badgeStyle = e.target.value;
        renderStyleClasses();
      });
    }

    // Theme selector radios
    document.querySelectorAll('input[name="layoutTheme"]').forEach((radio) => {
      radio.addEventListener("change", (e) => {
        state.theme = e.target.value;
        document.querySelectorAll(".theme-card").forEach((card) => {
          card.classList.toggle("active", card.querySelector("input").checked);
        });
        renderStyleClasses();
      });
    });

    // Checkboxes
    if (elements.chkShowBadgeGlow) {
      elements.chkShowBadgeGlow.addEventListener("change", (e) => {
        state.badgeGlow = e.target.checked;
        renderStyleClasses();
      });
    }

    if (elements.chkShowBgGrid) {
      elements.chkShowBgGrid.addEventListener("change", (e) => {
        state.bgGrid = e.target.checked;
        document.querySelectorAll(".art-grid-layer").forEach((el) => {
          el.style.display = state.bgGrid ? "block" : "none";
        });
      });
    }

    if (elements.chkShowPriceBox) {
      elements.chkShowPriceBox.addEventListener("change", (e) => {
        state.showPriceBox = e.target.checked;
        document.querySelectorAll(".art-price-badge").forEach((el) => {
          el.style.display = state.showPriceBox ? "flex" : "none";
        });
      });
    }
  }

  // --- Bind Color Pickers & Hex Inputs ---
  function bindColors() {
    const colorPairs = [
      { picker: elements.colorPrimary, hex: elements.hexPrimary, key: "primary", varName: "--brand-primary" },
      { picker: elements.colorSecondary, hex: elements.hexSecondary, key: "secondary", varName: "--brand-secondary" },
      { picker: elements.colorAccent, hex: elements.hexAccent, key: "accent", varName: "--brand-accent" },
      { picker: elements.colorBg, hex: elements.hexBg, key: "bg", varName: "--brand-bg" },
    ];

    colorPairs.forEach(({ picker, hex, key, varName }) => {
      if (!picker || !hex) return;

      picker.addEventListener("input", (e) => {
        const val = e.target.value.toUpperCase();
        hex.value = val;
        state.colors[key] = val;
        document.documentElement.style.setProperty(varName, val);
      });

      hex.addEventListener("input", (e) => {
        let val = e.target.value.trim();
        if (!val.startsWith("#")) val = "#" + val;
        if (/^#[0-9A-F]{6}$/i.test(val)) {
          picker.value = val;
          state.colors[key] = val;
          document.documentElement.style.setProperty(varName, val);
        }
      });
    });
  }

  // --- Presets (Brand Kit & Campaigns) ---
  function bindPresets() {
    // Brand preset buttons
    document.querySelectorAll(".brand-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        document.querySelectorAll(".brand-btn").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        applyBrandPreset(btn.dataset.brand);
      });
    });

    // Campaign preset dropdown
    if (elements.campaignPresetSelect) {
      elements.campaignPresetSelect.addEventListener("change", (e) => {
        const presetKey = e.target.value;
        if (presetKey && CAMPAIGN_PRESETS[presetKey]) {
          applyCampaignPreset(CAMPAIGN_PRESETS[presetKey]);
        }
      });
    }
  }

  function applyBrandPreset(brandKey) {
    const brand = BRAND_PRESETS[brandKey];
    if (!brand) return;

    state.brandName = brand.name;
    state.colors.primary = brand.primary;
    state.colors.secondary = brand.secondary;
    state.colors.accent = brand.accent;
    state.colors.bg = brand.bg;
    state.font = brand.font;

    // Update UI form elements
    if (elements.inputBrandName) elements.inputBrandName.value = brand.name;
    if (elements.colorPrimary) elements.colorPrimary.value = brand.primary;
    if (elements.hexPrimary) elements.hexPrimary.value = brand.primary;
    if (elements.colorSecondary) elements.colorSecondary.value = brand.secondary;
    if (elements.hexSecondary) elements.hexSecondary.value = brand.secondary;
    if (elements.colorAccent) elements.colorAccent.value = brand.accent;
    if (elements.hexAccent) elements.hexAccent.value = brand.accent;
    if (elements.colorBg) elements.colorBg.value = brand.bg;
    if (elements.hexBg) elements.hexBg.value = brand.bg;
    if (elements.fontSelect) elements.fontSelect.value = brand.font;

    // Apply CSS variables
    document.documentElement.style.setProperty("--brand-primary", brand.primary);
    document.documentElement.style.setProperty("--brand-secondary", brand.secondary);
    document.documentElement.style.setProperty("--brand-accent", brand.accent);
    document.documentElement.style.setProperty("--brand-bg", brand.bg);
    document.documentElement.style.setProperty("--brand-font", brand.font);

    renderContent();
  }

  function applyCampaignPreset(preset) {
    state.saleTag = preset.saleTag;
    state.discount = preset.discount;
    state.headline = preset.headline;
    state.subline = preset.subline;
    state.originalPrice = preset.originalPrice;
    state.offerPrice = preset.offerPrice;
    state.coupon = preset.coupon;
    state.cta = preset.cta;
    state.disclaimer = preset.disclaimer;
    state.badgeStyle = preset.badgeStyle;
    state.theme = preset.theme;
    state.selectedSampleHero = preset.hero;
    state.customHeroUrl = "";

    // Sync input controls
    if (elements.inputSaleTag) elements.inputSaleTag.value = preset.saleTag;
    if (elements.inputDiscount) elements.inputDiscount.value = preset.discount;
    if (elements.inputHeadline) elements.inputHeadline.value = preset.headline;
    if (elements.inputSubline) elements.inputSubline.value = preset.subline;
    if (elements.inputOriginalPrice) elements.inputOriginalPrice.value = preset.originalPrice;
    if (elements.inputOfferPrice) elements.inputOfferPrice.value = preset.offerPrice;
    if (elements.inputCouponCode) elements.inputCouponCode.value = preset.coupon;
    if (elements.inputCtaText) elements.inputCtaText.value = preset.cta;
    if (elements.inputDisclaimer) elements.inputDisclaimer.value = preset.disclaimer;
    if (elements.badgeStyleSelect) elements.badgeStyleSelect.value = preset.badgeStyle;

    // Radio
    const radio = document.querySelector(`input[name="layoutTheme"][value="${preset.theme}"]`);
    if (radio) {
      radio.checked = true;
      document.querySelectorAll(".theme-card").forEach((card) => {
        card.classList.toggle("active", card.querySelector("input").checked);
      });
    }

    // Hero buttons
    document.querySelectorAll(".sample-hero-btn").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.img === preset.hero);
    });

    if (preset.brandPreset) {
      applyBrandPreset(preset.brandPreset);
      document.querySelectorAll(".brand-btn").forEach((b) => {
        b.classList.toggle("active", b.dataset.brand === preset.brandPreset);
      });
    }

    renderContent();
    renderStyleClasses();
    renderHero();
    showToast(`Loaded "${preset.saleTag}" Preset!`);
  }

  // --- Hero & Cutout Image Controls ---
  function bindHeroControls() {
    // Custom Hero File Upload
    if (elements.inputHeroImage) {
      elements.inputHeroImage.addEventListener("change", (e) => {
        const file = e.target.files[0];
        if (file) {
          const reader = new FileReader();
          reader.onload = (evt) => {
            state.customHeroUrl = evt.target.result;
            document.querySelectorAll(".sample-hero-btn").forEach((b) => b.classList.remove("active"));
            renderHero();
            showToast("Custom Hero Image Uploaded!");
          };
          reader.readAsDataURL(file);
        }
      });
    }

    // Sample hero buttons
    document.querySelectorAll(".sample-hero-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        document.querySelectorAll(".sample-hero-btn").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        state.selectedSampleHero = btn.dataset.img;
        state.customHeroUrl = "";
        if (elements.inputHeroImage) elements.inputHeroImage.value = "";
        renderHero();
      });
    });

    // Slider scale
    if (elements.sliderHeroScale) {
      elements.sliderHeroScale.addEventListener("input", (e) => {
        const val = Number(e.target.value);
        state.heroScale = val / 100;
        if (elements.valHeroScale) elements.valHeroScale.textContent = `${val}%`;
        document.documentElement.style.setProperty("--hero-scale", state.heroScale);
      });
    }

    // Slider vertical pos
    if (elements.sliderHeroPosY) {
      elements.sliderHeroPosY.addEventListener("input", (e) => {
        const val = Number(e.target.value);
        state.heroPosY = val;
        if (elements.valHeroPosY) elements.valHeroPosY.textContent = `${val}px`;
        document.documentElement.style.setProperty("--hero-pos-y", `${val}px`);
      });
    }

    // Hero glow
    if (elements.chkHeroGlow) {
      elements.chkHeroGlow.addEventListener("change", (e) => {
        state.heroGlow = e.target.checked;
        document.querySelectorAll(".art-hero-glow").forEach((el) => {
          el.style.display = state.heroGlow ? "block" : "none";
        });
      });
    }

    // Logo upload
    if (elements.inputBrandLogo) {
      elements.inputBrandLogo.addEventListener("change", (e) => {
        const file = e.target.files[0];
        if (file) {
          const reader = new FileReader();
          reader.onload = (evt) => {
            state.brandLogoUrl = evt.target.result;
            if (elements.logoPreviewImg) {
              elements.logoPreviewImg.src = state.brandLogoUrl;
              elements.logoPreviewImg.style.display = "block";
            }
            document.querySelectorAll(".art-logo-img").forEach((img) => {
              img.src = state.brandLogoUrl;
              img.style.display = "block";
            });
            showToast("Brand Logo Updated!");
          };
          reader.readAsDataURL(file);
        }
      });
    }

    // Reset logo
    if (elements.btnResetLogo) {
      elements.btnResetLogo.addEventListener("click", () => {
        state.brandLogoUrl = "/assets/testbook-logo.png";
        if (elements.inputBrandLogo) elements.inputBrandLogo.value = "";
        if (elements.logoPreviewImg) elements.logoPreviewImg.src = state.brandLogoUrl;
        document.querySelectorAll(".art-logo-img").forEach((img) => {
          img.src = state.brandLogoUrl;
        });
        showToast("Reset to Default Logo");
      });
    }
  }

  // --- Zoom Controls ---
  function bindZoomControls() {
    if (elements.btnZoomIn) {
      elements.btnZoomIn.addEventListener("click", () => {
        setZoom(Math.min(1.4, state.zoom + 0.1));
      });
    }

    if (elements.btnZoomOut) {
      elements.btnZoomOut.addEventListener("click", () => {
        setZoom(Math.max(0.4, state.zoom - 0.1));
      });
    }

    if (elements.btnResetZoom) {
      elements.btnResetZoom.addEventListener("click", () => {
        setZoom(0.8);
      });
    }
  }

  function setZoom(val) {
    state.zoom = Math.round(val * 100) / 100;
    if (elements.zoomLevelDisplay) {
      elements.zoomLevelDisplay.textContent = `${Math.round(state.zoom * 100)}%`;
    }
    document.documentElement.style.setProperty("--preview-zoom", state.zoom);
  }

  // --- Render Functions ---
  function updateUI() {
    renderContent();
    renderStyleClasses();
    renderHero();
  }

  function renderContent() {
    // Update all artboard texts
    document.querySelectorAll(".art-tag-text").forEach((el) => (el.textContent = state.saleTag));
    document.querySelectorAll(".art-discount-text").forEach((el) => (el.textContent = state.discount));
    document.querySelectorAll(".art-headline-text").forEach((el) => (el.textContent = state.headline));
    document.querySelectorAll(".art-subline-text").forEach((el) => (el.textContent = state.subline));
    document.querySelectorAll(".art-price-orig").forEach((el) => (el.textContent = state.originalPrice));
    document.querySelectorAll(".art-price-now").forEach((el) => (el.textContent = state.offerPrice));
    document.querySelectorAll(".art-coupon-text").forEach((el) => (el.textContent = state.coupon));
    document.querySelectorAll(".art-cta-text").forEach((el) => (el.textContent = state.cta));
    document.querySelectorAll(".art-disclaimer-text").forEach((el) => (el.textContent = state.disclaimer));
    document.querySelectorAll(".art-brand-text").forEach((el) => (el.textContent = state.brandName));
  }

  function renderStyleClasses() {
    const container = document.getElementById("previewsContainer");
    if (!container) return;

    // Clear previous theme classes
    container.className = "previews-container";
    container.classList.add(state.theme);
    container.classList.add(state.badgeStyle);

    if (state.badgeGlow) {
      container.classList.add("badge-glow-active");
    }
  }

  function renderHero() {
    const heroImgs = document.querySelectorAll(".art-hero-img");
    const heroFallbacks = document.querySelectorAll(".art-hero-svg-fallback");

    if (state.customHeroUrl) {
      heroImgs.forEach((img) => {
        img.src = state.customHeroUrl;
        img.style.display = "block";
      });
      heroFallbacks.forEach((fb) => (fb.style.display = "none"));
    } else {
      const emoji = SAMPLE_HERO_EMOJIS[state.selectedSampleHero] || "👨‍🏫";
      heroImgs.forEach((img) => (img.style.display = "none"));
      heroFallbacks.forEach((fb) => {
        fb.textContent = emoji;
        fb.style.display = "block";
      });
    }
  }

  // --- Toast Notification ---
  function showToast(message) {
    if (!elements.toast) return;
    elements.toast.textContent = message;
    elements.toast.classList.add("show");
    clearTimeout(elements.toastTimeout);
    elements.toastTimeout = setTimeout(() => {
      elements.toast.classList.remove("show");
    }, 2800);
  }

  // --- High-Resolution Exporter Pipeline ---
  function bindExportActions() {
    // Individual Download Buttons
    document.querySelectorAll(".btn-download").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const targetId = btn.dataset.target;
        const filename = btn.dataset.filename || `${targetId}.png`;
        const artboard = document.getElementById(targetId);
        if (!artboard) return;

        btn.disabled = true;
        const origText = btn.textContent;
        btn.textContent = "⏳ Generating...";
        try {
          const blob = await exportArtboardToBlob(artboard);
          downloadBlob(blob, filename);
          showToast(`Downloaded ${filename} successfully!`);
        } catch (err) {
          console.error("Export error:", err);
          showToast("Export failed. Please try again.");
        } finally {
          btn.disabled = false;
          btn.textContent = origText;
        }
      });
    });

    // Copy to Clipboard Buttons
    document.querySelectorAll(".btn-copy").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const targetId = btn.dataset.target;
        const artboard = document.getElementById(targetId);
        if (!artboard) return;

        btn.disabled = true;
        const origText = btn.textContent;
        btn.textContent = "⏳...";
        try {
          const blob = await exportArtboardToBlob(artboard);
          if (navigator.clipboard && navigator.clipboard.write) {
            const item = new ClipboardItem({ "image/png": blob });
            await navigator.clipboard.write([item]);
            showToast("Copied creative to clipboard! 📋");
          } else {
            showToast("Clipboard API not supported. Use PNG download.");
          }
        } catch (err) {
          console.error("Copy error:", err);
          showToast("Failed to copy image.");
        } finally {
          btn.disabled = false;
          btn.textContent = origText;
        }
      });
    });

    // Batch Export All 4 Sizes (.ZIP)
    if (elements.btnExportAll) {
      elements.btnExportAll.addEventListener("click", async () => {
        if (typeof JSZip === "undefined") {
          showToast("JSZip library loading... please wait.");
          return;
        }

        elements.btnExportAll.disabled = true;
        const origHtml = elements.btnExportAll.innerHTML;
        elements.btnExportAll.innerHTML = `<span>⏳ Rendering 4 Creatives...</span>`;

        try {
          const zip = new JSZip();
          const folder = zip.folder("testbook-sale-creatives");

          const items = [
            { id: "canvas-1x1", name: "sale-creative-1x1-feed-1080x1080.png" },
            { id: "canvas-9x16", name: "sale-creative-9x16-story-1080x1920.png" },
            { id: "canvas-16x9", name: "sale-creative-16x9-banner-1920x1080.png" },
            { id: "canvas-4x3", name: "sale-creative-4x3-display-1200x900.png" },
          ];

          for (const item of items) {
            const artboard = document.getElementById(item.id);
            if (artboard) {
              const blob = await exportArtboardToBlob(artboard);
              folder.file(item.name, blob);
            }
          }

          const content = await zip.generateAsync({ type: "blob" });
          downloadBlob(content, `sale-creatives-${Date.now()}.zip`);
          showToast("🎉 All 4 Creatives packaged and downloaded in .ZIP!");
        } catch (err) {
          console.error("Batch export error:", err);
          showToast("Batch export failed. Try single downloads.");
        } finally {
          elements.btnExportAll.disabled = false;
          elements.btnExportAll.innerHTML = origHtml;
        }
      });
    }
  }

  /**
   * High-Resolution Canvas snapshot using html2canvas / DOM Cloning
   */
  async function exportArtboardToBlob(artboard) {
    if (typeof html2canvas !== "undefined") {
      // Temporarily remove preview transform scaling for native high-res render
      const originalTransform = artboard.style.transform;
      artboard.style.transform = "none";

      const targetWidth = artboard.offsetWidth;
      const targetHeight = artboard.offsetHeight;

      const canvas = await html2canvas(artboard, {
        width: targetWidth,
        height: targetHeight,
        scale: 1, // Full 1:1 pixel fidelity
        useCORS: true,
        allowTaint: true,
        backgroundColor: null,
        logging: false,
      });

      artboard.style.transform = originalTransform;

      return new Promise((resolve) => {
        canvas.toBlob((blob) => resolve(blob), "image/png", 1.0);
      });
    } else {
      // Fallback SVG foreignObject canvas renderer
      return exportViaSvgForeignObject(artboard);
    }
  }

  function exportViaSvgForeignObject(artboard) {
    return new Promise((resolve, reject) => {
      const width = artboard.offsetWidth;
      const height = artboard.offsetHeight;
      const html = new XMLSerializer().serializeToString(artboard);

      const svgString = `
        <svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}">
          <foreignObject width="100%" height="100%">
            <div xmlns="http://www.w3.org/1999/xhtml" style="width:100%;height:100%;">
              ${html}
            </div>
          </foreignObject>
        </svg>
      `;

      const img = new Image();
      const svgBlob = new Blob([svgString], { type: "image/svg+xml;charset=utf-8" });
      const url = URL.createObjectURL(svgBlob);

      img.onload = () => {
        const canvas = document.createElement("canvas");
        canvas.width = width;
        canvas.height = height;
        const ctx = canvas.getContext("2d");
        ctx.drawImage(img, 0, 0);
        URL.revokeObjectURL(url);
        canvas.toBlob((blob) => resolve(blob), "image/png");
      };
      img.onerror = (e) => reject(e);
      img.src = url;
    });
  }

  function downloadBlob(blob, filename) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  // --- Launch on DOM Ready ---
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
