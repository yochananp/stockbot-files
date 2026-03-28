# ════════════════════════════════════════════════════════════
# TAB 3 — OPTIONS ANALYSIS v2
# ════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Options Analysis")
    st.caption("Fetches live option chains from yfinance · Greeks via Black-Scholes · Strategy based on SV Alert + IV Rank")

    with st.expander("IV Rank — what it means and how to use it"):
        st.markdown("""
**IV Rank** measures where the current Implied Volatility sits relative to the term structure (short vs long-dated options).
> ⚠️ Note: IV Rank here is based on the **options term structure**, not historical IV data.

| IV Rank | Label | Meaning | Best Strategies |
|---------|-------|---------|-----------------|
| 0–20 | **Very Low IV** | Options are cheap. | Long Call, Long Put, Bull/Bear Call Spread |
| 20–40 | **Low IV** | Slightly below average. Favors buying. | Long Call, Bull Call Spread |
| 40–60 | **Neutral IV** | Both buying and selling viable. | Covered Call, Cash-Secured Put |
| 60–80 | **High IV** | Options expensive. Sellers have edge. | Covered Call, Cash-Secured Put, Iron Condor |
| 80–100 | **Very High IV** | Very expensive — near earnings/events. | Cash-Secured Put, Iron Condor |

**Strategy logic:** IV Rank < 50 → buy premium · IV Rank > 50 → sell premium
        """)

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

    # ── Global IV Override ────────────────────────────────────
    st.markdown("---")
    iv_col1, iv_col2 = st.columns([2, 4])
    global_iv_override = iv_col1.number_input(
        "Global IV% override (0 = use live data)",
        min_value=0.0, max_value=200.0, value=0.0, step=1.0,
        key="opt_global_iv",
        help="Enter a value to override IV for ALL tickers. Set to 0 to use live yfinance data."
    )
    if global_iv_override > 0:
        iv_col2.info(f"Using manual IV: **{global_iv_override:.1f}%** for all tickers")

    # ── Ticker input ──────────────────────────────────────────
    opt_tickers   = []
    opt_alert_map = {}

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
            opt_tickers   = [r["ticker"] for r in sv_rows]
            opt_alert_map = {r["ticker"]: r.get("alert", "NEUTRAL") for r in sv_rows}
            st.caption(f"Loaded {len(opt_tickers)} tickers from sv_results — {sel_date}")
        else:
            st.warning("No SV results found. Run the pipeline first.")

    elif opt_source == "Custom List":
        all_lists = get_lists()
        if not all_lists:
            st.warning("No custom lists found.")
        else:
            list_options   = {f"{l['name']} ({l['ticker_count']} tickers)": l["id"] for l in all_lists}
            sel_list_label = st.selectbox("Select list", list(list_options.keys()), key="opt_list_sel")
            sel_list_id    = list_options[sel_list_label]
            list_tickers   = get_list_tickers(sel_list_id)
            opt_tickers    = [t["ticker"] for t in list_tickers]
            opt_alert_map  = {t["ticker"]: t.get("sv_alert", "NEUTRAL") or "NEUTRAL" for t in list_tickers}
            st.caption(f"Loaded {len(opt_tickers)} tickers from list")

    else:
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

    # ── Per-ticker IV override table ──────────────────────────
    if opt_tickers:
        st.markdown("#### Per-ticker IV override (optional)")
        st.caption("Edit IV% per ticker to override live data. Leave 0 to use live yfinance data or global override above.")

        iv_df = pd.DataFrame({
            "Ticker": opt_tickers[:30],
            "IV% Override": [global_iv_override] * min(len(opt_tickers), 30),
        })

        edited_iv = st.data_editor(
            iv_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Ticker":      st.column_config.TextColumn("Ticker",      width=100, disabled=True),
                "IV% Override": st.column_config.NumberColumn("IV% Override", width=120,
                                                               min_value=0.0, max_value=200.0,
                                                               format="%.1f%%",
                                                               help="0 = use live data"),
            },
            key="opt_iv_editor",
        )
        # Build per-ticker IV map
        iv_override_map = dict(zip(edited_iv["Ticker"], edited_iv["IV% Override"]))
    else:
        iv_override_map = {}

    # ── Analyze button ────────────────────────────────────────
    st.markdown("---")
    col_btn1, col_btn2 = st.columns([1, 4])
    max_tickers = col_btn2.slider(
        "Max tickers to analyze (yfinance is slow)",
        min_value=1, max_value=30, value=10, key="opt_max_n"
    )
    run_options = col_btn1.button(
        "Analyze Options", type="primary",
        key="btn_run_options",
        disabled=len(opt_tickers) == 0 or len(opt_buckets) == 0,
    )

    if run_options and opt_tickers:
        tickers_to_run = opt_tickers[:max_tickers]
        results        = []
        progress_bar   = st.progress(0)
        status_text    = st.empty()

        for i, ticker in enumerate(tickers_to_run):
            status_text.caption(f"Analyzing {ticker} ({i+1}/{len(tickers_to_run)})...")
            alert      = opt_alert_map.get(ticker, "NEUTRAL")
            iv_override = iv_override_map.get(ticker, 0.0)
            res        = analyze_ticker_options(ticker, alert, opt_buckets,
                                                iv_override=iv_override if iv_override > 0 else None)
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
            colors = {"BUY": "#0d2b18", "CAUTION": "#2b1a00", "WATCH": "#2b2600", "NEUTRAL": "#1a1a1a"}
            return f"background-color: {colors.get(val, '#1a1a1a')}"

        def _style_iv_rank(val):
            if val is None:
                return ""
            if val > 70:
                return "color: #ef5350; font-weight: bold"
            elif val > 50:
                return "color: #ffa726"
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
                dm1, dm2, dm3, dm4, dm5 = st.columns(5)
                dm1.metric("Spot",     f"${drill['spot']:.2f}")
                dm2.metric("Alert",    drill["alert"])
                dm3.metric("IV%",      f"{drill['iv_current']:.1f}%" if drill['iv_current'] else "—")
                dm4.metric("IV Rank",  f"{drill['iv_rank']:.0f}" if drill['iv_rank'] is not None else "—")
                dm5.metric("Strategy", drill["strategy"])

                st.caption(f"Rationale: {drill['rationale']}")
                rp = drill.get("risk_profile", {})
                rc1, rc2, rc3 = st.columns(3)
                rc1.info(f"**Max Profit:** {rp.get('max_profit','—')}")
                rc2.error(f"**Max Risk:** {rp.get('max_risk','—')}")
                rc3.warning(f"**Breakeven:** {rp.get('breakeven','—')}")

                contracts  = drill.get("contracts", [])
                strategy   = drill.get("strategy", "")
                is_spread  = "Spread" in strategy

                if is_spread and len(contracts) >= 2:
                    st.markdown("#### Spread Legs")
                    buy_leg  = contracts[0]
                    sell_leg = contracts[1] if len(contracts) > 1 else None

                    if sell_leg:
                        net_debit    = round(buy_leg["mid"] - sell_leg["mid"], 2)
                        spread_width = round(abs(sell_leg["strike"] - buy_leg["strike"]), 2)
                        max_profit   = round(spread_width - net_debit, 2) if net_debit > 0 else round(net_debit - spread_width, 2)
                        breakeven    = round(buy_leg["strike"] + net_debit, 2) if "Bull" in strategy else round(buy_leg["strike"] - net_debit, 2)

                        sp1, sp2, sp3, sp4 = st.columns(4)
                        sp1.metric("Net Debit",    f"${net_debit:.2f}")
                        sp2.metric("Max Profit",   f"${max_profit:.2f}")
                        sp3.metric("Max Risk",     f"${net_debit:.2f}")
                        sp4.metric("Breakeven",    f"${breakeven:.2f}")

                        leg_data = []
                        for leg, label in [(buy_leg, "BUY"), (sell_leg, "SELL")]:
                            leg_data.append({
                                "Leg":      label,
                                "Expiry":   leg["expiry"],
                                "Strike":   leg["strike"],
                                "Type":     leg["type"],
                                "Bid":      leg["bid"],
                                "Ask":      leg["ask"],
                                "Mid":      leg["mid"],
                                "Delta":    leg["delta"],
                                "Theta":    leg["theta"],
                                "IV%":      leg["iv_pct"],
                                "OI":       leg["oi"],
                            })
                        df_legs = pd.DataFrame(leg_data)

                        def _style_leg(val):
                            if val == "BUY":
                                return "color: #26a69a; font-weight: bold"
                            elif val == "SELL":
                                return "color: #ef5350; font-weight: bold"
                            return ""

                        st.dataframe(
                            df_legs.style.applymap(_style_leg, subset=["Leg"])
                            .format({
                                "Strike": "${:.2f}", "Bid": "${:.2f}",
                                "Ask": "${:.2f}", "Mid": "${:.2f}",
                                "Delta": "{:.4f}", "Theta": "{:.4f}",
                                "IV%": "{:.1f}%",
                            }, na_rep="—"),
                            use_container_width=True,
                            hide_index=True,
                            height=120,
                        )

                st.markdown("#### All Recommended Contracts")
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
                        if 0.30 <= abs(val) <= 0.50:
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

        # ── Export to Google Sheets ───────────────────────────
        st.markdown("---")
        st.markdown("### Export to Google Sheets")
        exp1, exp2 = st.columns([3, 1])
        owner_email = exp1.text_input(
            "Your Google email (to share the sheet with you)",
            placeholder="you@gmail.com",
            key="opt_owner_email",
        )
        if exp2.button("Export to Sheets", type="primary", key="btn_export_sheets",
                       disabled="opt_results" not in st.session_state):
            results = st.session_state.get("opt_results", [])
            valid   = [r for r in results if not r.get("error")]
            if not valid:
                st.warning("No valid results to export.")
            else:
                with st.spinner("Creating Google Sheet..."):
                    try:
                        from skills.sheets_exporter import export_options_to_sheets
                        url = export_options_to_sheets(valid, owner_email or None)
                        st.success("Sheet created!")
                        st.markdown(f"[Open Google Sheet]({url})")
                    except Exception as e:
                        st.error(f"Export failed: {e}")
