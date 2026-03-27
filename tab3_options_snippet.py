# ════════════════════════════════════════════════════════════
# TAB 3 — OPTIONS ANALYSIS
# ════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Options Analysis")
    st.caption("Fetches live option chains from yfinance · Greeks via Black-Scholes · Strategy based on SV Alert + IV Rank")

    from skills.options_engine import analyze_ticker_options, EXPIRY_BUCKETS

    # ── Controls ──────────────────────────────────────────────
    oc1, oc2, oc3 = st.columns([3, 2, 2])

    with oc1:
        opt_source = st.radio(
            "Ticker source",
            ["SV Results (latest date)", "Custom List", "Manual input"],
            horizontal=True,
            key="opt_source",
        )

    with oc2:
        opt_alert_src = st.radio(
            "Alert source",
            ["sv_results", "alerts table"],
            horizontal=True,
            key="opt_alert_src",
        )

    with oc3:
        opt_buckets = st.multiselect(
            "Expiry range",
            list(EXPIRY_BUCKETS.keys()),
            default=["60-90d", "LEAPS"],
            key="opt_buckets",
        )

    # ── Ticker input depending on source ─────────────────────
    opt_tickers  = []
    opt_alert_map = {}  # ticker → alert

    if opt_source == "SV Results (latest date)":
        sv_dates = get_available_dates()
        sel_date = sv_dates[0] if sv_dates else None
        if sel_date:
            sv_rows = get_all_results(run_date=sel_date, timeframe="1D")
            opt_alert_filter = st.selectbox(
                "Filter by alert", ["ALL", "BUY", "CAUTION", "WATCH", "NEUTRAL"],
                key="opt_sv_alert_filter"
            )
            if opt_alert_filter != "ALL":
                sv_rows = [r for r in sv_rows if r.get("alert") == opt_alert_filter]
            opt_tickers  = [r["ticker"] for r in sv_rows]
            opt_alert_map = {r["ticker"]: r.get("alert", "NEUTRAL") for r in sv_rows}
            st.caption(f"Loaded {len(opt_tickers)} tickers from sv_results — {sel_date}")
        else:
            st.warning("No SV results found. Run the pipeline first.")

    elif opt_source == "Custom List":
        all_lists = get_lists()
        if not all_lists:
            st.warning("No custom lists found.")
        else:
            list_options = {f"{l['name']} ({l['ticker_count']} tickers)": l["id"] for l in all_lists}
            sel_list_label = st.selectbox("Select list", list(list_options.keys()), key="opt_list_sel")
            sel_list_id    = list_options[sel_list_label]
            list_tickers   = get_list_tickers(sel_list_id)
            opt_tickers    = [t["ticker"] for t in list_tickers]
            # sv_alert from list data if available
            opt_alert_map  = {t["ticker"]: t.get("sv_alert", "NEUTRAL") or "NEUTRAL" for t in list_tickers}
            st.caption(f"Loaded {len(opt_tickers)} tickers from list")

    else:  # Manual
        raw_input = st.text_area(
            "Tickers (comma-separated)",
            placeholder="AAPL, MSFT, NVDA",
            height=80,
            key="opt_manual_tickers",
        )
        manual_alert = st.selectbox(
            "Alert for all manual tickers",
            ["BUY", "CAUTION", "WATCH", "NEUTRAL"],
            key="opt_manual_alert",
        )
        opt_tickers   = [t.strip().upper() for t in raw_input.split(",") if t.strip()]
        opt_alert_map = {t: manual_alert for t in opt_tickers}

    # ── Analyze button ────────────────────────────────────────
    st.markdown("---")
    col_btn1, col_btn2 = st.columns([1, 4])
    max_tickers = col_btn2.slider(
        "Max tickers to analyze (yfinance is slow)",
        min_value=1, max_value=30, value=10, key="opt_max_n"
    )
    run_options = col_btn1.button(
        "🔍 Analyze Options", type="primary",
        key="btn_run_options",
        disabled=len(opt_tickers) == 0 or len(opt_buckets) == 0,
    )

    if run_options and opt_tickers:
        tickers_to_run = opt_tickers[:max_tickers]
        results = []
        progress_bar = st.progress(0)
        status_text  = st.empty()

        for i, ticker in enumerate(tickers_to_run):
            status_text.caption(f"Analyzing {ticker} ({i+1}/{len(tickers_to_run)})...")
            alert = opt_alert_map.get(ticker, "NEUTRAL")
            res   = analyze_ticker_options(ticker, alert, opt_buckets)
            results.append(res)
            progress_bar.progress((i + 1) / len(tickers_to_run))

        progress_bar.empty()
        status_text.empty()
        st.session_state["opt_results"] = results
        st.success(f"Analysis complete — {len(results)} tickers")

    # ── Display results ───────────────────────────────────────
    if "opt_results" in st.session_state:
        results = st.session_state["opt_results"]

        # ── SUMMARY TABLE ─────────────────────────────────────
        st.markdown("### Summary")
        summary_rows = []
        for r in results:
            if r.get("error"):
                summary_rows.append({
                    "Ticker":    r["ticker"],
                    "Alert":     r.get("alert", "—"),
                    "Spot":      None,
                    "IV%":       None,
                    "IV Rank":   None,
                    "Strategy":  "ERROR",
                    "Rationale": r["error"],
                    "Max Profit":"—",
                    "Max Risk":  "—",
                    "Breakeven": "—",
                })
            else:
                rp = r.get("risk_profile", {})
                summary_rows.append({
                    "Ticker":    r["ticker"],
                    "Alert":     r.get("alert", "—"),
                    "Spot":      r.get("spot"),
                    "IV%":       r.get("iv_current"),
                    "IV Rank":   r.get("iv_rank"),
                    "Strategy":  r.get("strategy", "—"),
                    "Rationale": r.get("rationale", "—"),
                    "Max Profit":rp.get("max_profit", "—"),
                    "Max Risk":  rp.get("max_risk",   "—"),
                    "Breakeven": rp.get("breakeven",  "—"),
                })

        df_summary = pd.DataFrame(summary_rows)

        def _style_opt_alert(val):
            colors = {
                "BUY":     "#0d2b18",
                "CAUTION": "#2b1a00",
                "WATCH":   "#2b2600",
                "NEUTRAL": "#1a1a1a",
            }
            return f"background-color: {colors.get(val, '#1a1a1a')}"

        def _style_iv_rank(val):
            if val is None:
                return ""
            if val > 70:
                return "color: #ef5350; font-weight: bold"
            elif val > 50:
                return "color: #ffa726"
            else:
                return "color: #26a69a"

        styled_summary = (
            df_summary.style
            .applymap(_style_opt_alert, subset=["Alert"])
            .applymap(_style_iv_rank,   subset=["IV Rank"])
            .format({
                "Spot":    "${:.2f}",
                "IV%":     "{:.1f}%",
                "IV Rank": "{:.0f}",
            }, na_rep="—")
        )

        st.dataframe(
            styled_summary,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Ticker":    st.column_config.TextColumn("Ticker",    width=80),
                "Alert":     st.column_config.TextColumn("Alert",     width=90),
                "Spot":      st.column_config.NumberColumn("Spot",    width=90,  format="$%.2f"),
                "IV%":       st.column_config.NumberColumn("IV%",     width=70,  format="%.1f%%"),
                "IV Rank":   st.column_config.NumberColumn("IV Rank", width=80,  format="%.0f"),
                "Strategy":  st.column_config.TextColumn("Strategy",  width=160),
                "Rationale": st.column_config.TextColumn("Rationale", width=260),
                "Max Profit":st.column_config.TextColumn("Max Profit",width=160),
                "Max Risk":  st.column_config.TextColumn("Max Risk",  width=160),
                "Breakeven": st.column_config.TextColumn("Breakeven", width=130),
            },
            height=400,
        )

        # ── DRILL DOWN ────────────────────────────────────────
        st.markdown("---")
        st.markdown("### Drill Down — Recommended Contracts")

        valid_results = [r for r in results if not r.get("error") and r.get("contracts")]
        if not valid_results:
            st.info("No contracts found. Check ticker symbols or expand expiry range.")
        else:
            drill_ticker = st.selectbox(
                "Select ticker for drill down",
                [r["ticker"] for r in valid_results],
                key="opt_drill_ticker",
            )
            drill = next((r for r in valid_results if r["ticker"] == drill_ticker), None)

            if drill:
                # Header metrics
                dm1, dm2, dm3, dm4, dm5 = st.columns(5)
                dm1.metric("Spot",     f"${drill['spot']:.2f}")
                dm2.metric("Alert",    drill["alert"])
                dm3.metric("IV%",      f"{drill['iv_current']:.1f}%" if drill['iv_current'] else "—")
                dm4.metric("IV Rank",  f"{drill['iv_rank']:.0f}" if drill['iv_rank'] is not None else "—")
                dm5.metric("Strategy", drill["strategy"])

                st.caption(f"📌 Rationale: {drill['rationale']}")
                rp = drill.get("risk_profile", {})
                rc1, rc2, rc3 = st.columns(3)
                rc1.info(f"**Max Profit:** {rp.get('max_profit','—')}")
                rc2.error(f"**Max Risk:** {rp.get('max_risk','—')}")
                rc3.warning(f"**Breakeven:** {rp.get('breakeven','—')}")

                st.markdown("#### Recommended Contracts")
                contracts = drill.get("contracts", [])
                if contracts:
                    df_contracts = pd.DataFrame(contracts)
                    display_cols = [
                        "expiry", "dte", "strike", "type",
                        "bid", "ask", "mid", "iv_pct", "oi", "volume", "spread_pct",
                        "delta", "gamma", "theta", "vega", "bs_price",
                    ]
                    df_contracts = df_contracts[[c for c in display_cols if c in df_contracts.columns]]
                    df_contracts.columns = [c.replace("_", " ").title() for c in df_contracts.columns]

                    def _style_delta(val):
                        if val is None:
                            return ""
                        abs_val = abs(val)
                        if 0.30 <= abs_val <= 0.50:
                            return "color: #26a69a; font-weight: bold"
                        return ""

                    def _style_spread(val):
                        if val is None:
                            return ""
                        if val > 10:
                            return "color: #ef5350"
                        elif val > 5:
                            return "color: #ffa726"
                        return "color: #26a69a"

                    styled_contracts = (
                        df_contracts.style
                        .applymap(_style_delta,  subset=["Delta"])
                        .applymap(_style_spread, subset=["Spread Pct"])
                        .format({
                            "Strike":     "${:.2f}",
                            "Bid":        "${:.2f}",
                            "Ask":        "${:.2f}",
                            "Mid":        "${:.2f}",
                            "Iv Pct":     "{:.1f}%",
                            "Spread Pct": "{:.1f}%",
                            "Delta":      "{:.4f}",
                            "Gamma":      "{:.6f}",
                            "Theta":      "{:.4f}",
                            "Vega":       "{:.4f}",
                            "Bs Price":   "${:.2f}",
                        }, na_rep="—")
                    )

                    st.dataframe(
                        styled_contracts,
                        use_container_width=True,
                        hide_index=True,
                        height=200,
                    )

                    # Greeks explanation
                    with st.expander("Greeks reference"):
                        st.markdown("""
| Greek | Meaning |
|-------|---------|
| **Delta** | Price change per $1 move in stock. ~0.40 = sweet spot for directional trades |
| **Gamma** | Rate of delta change — high near expiry |
| **Theta** | Daily time decay (negative = you lose this per day if held) |
| **Vega**  | Price change per 1% change in IV — high for LEAPS |
| **BS Price** | Theoretical fair value from Black-Scholes |
| **IV%** | Implied volatility of this contract |
| **Spread%** | Bid/ask spread as % of mid — below 5% is good liquidity |
                        """)
                else:
                    st.warning("No contracts found for this ticker in the selected expiry range.")
