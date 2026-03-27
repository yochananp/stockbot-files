# ════════════════════════════════════════════════════════════
# TAB 4 — PATTERNS
# ════════════════════════════════════════════════════════════
with tab4:
    st.subheader("Pattern Detection")
    st.caption("Scans for Cup & Handle, Head & Shoulders, and Inverse H&S — 1D daily data")

    from skills.pattern_engine import detect_patterns

    # ── Input Panel ───────────────────────────────────────────
    pc1, pc2, pc3 = st.columns([3, 2, 2])

    with pc1:
        pat_source = st.radio(
            "Ticker source",
            ["Custom List", "SV Results (latest)", "Manual input"],
            horizontal=True,
            key="pat_source",
        )

    with pc2:
        pat_lookback = st.slider(
            "Lookback (days)", 60, 252, 252, key="pat_lookback"
        )

    with pc3:
        pat_min_conf = st.slider(
            "Min confidence %", 0, 90, 40, key="pat_min_conf"
        )

    # ── Ticker list ───────────────────────────────────────────
    pat_tickers = []

    if pat_source == "Custom List":
        all_lists = get_lists()
        if not all_lists:
            st.warning("No custom lists found.")
        else:
            list_opts  = {f"{l['name']} ({l['ticker_count']} tickers)": l["id"] for l in all_lists}
            sel_label  = st.selectbox("Select list", list(list_opts.keys()), key="pat_list_sel")
            sel_id     = list_opts[sel_label]
            list_data  = get_list_tickers(sel_id)
            pat_tickers = [t["ticker"] for t in list_data]
            st.caption(f"Loaded {len(pat_tickers)} tickers")

    elif pat_source == "SV Results (latest)":
        sv_dates = get_available_dates()
        if sv_dates:
            sv_rows    = get_all_results(run_date=sv_dates[0], timeframe="1D")
            pat_tickers = [r["ticker"] for r in sv_rows]
            st.caption(f"Loaded {len(pat_tickers)} tickers from SV Results — {sv_dates[0]}")
        else:
            st.warning("No SV results found.")

    else:
        raw = st.text_area(
            "Tickers (comma-separated)",
            placeholder="AAPL, MSFT, NVDA",
            height=80,
            key="pat_manual",
        )
        pat_tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]

    # ── Scan button ───────────────────────────────────────────
    st.markdown("---")
    btn_col, info_col = st.columns([1, 4])
    max_pat = info_col.slider("Max tickers to scan", 1, 50, 20, key="pat_max_n")
    run_scan = btn_col.button(
        "🔍 Scan Patterns",
        type="primary",
        key="btn_scan_patterns",
        disabled=len(pat_tickers) == 0,
    )

    if run_scan and pat_tickers:
        tickers_to_scan = pat_tickers[:max_pat]
        scan_results    = []
        prog            = st.progress(0)
        status          = st.empty()

        for i, ticker in enumerate(tickers_to_scan):
            status.caption(f"Scanning {ticker} ({i+1}/{len(tickers_to_scan)})...")
            try:
                df_ohlcv = fetch_ohlcv(ticker, "1D")
                if df_ohlcv is None or len(df_ohlcv) < 60:
                    scan_results.append({
                        "ticker":  ticker,
                        "pattern": "NO DATA",
                        "direction": "—",
                        "confidence": 0,
                        "current": None,
                        "target":  None,
                        "pct_to_target": None,
                        "_df": None,
                        "_result": None,
                    })
                else:
                    result = detect_patterns(df_ohlcv, lookback=pat_lookback)
                    scan_results.append({
                        "ticker":        ticker,
                        "pattern":       result.get("pattern") or "None",
                        "direction":     result.get("direction", "—"),
                        "confidence":    result.get("confidence", 0),
                        "current":       result.get("current"),
                        "target":        result.get("target"),
                        "pct_to_target": result.get("pct_to_target"),
                        "_df":           df_ohlcv,
                        "_result":       result,
                    })
            except Exception as e:
                scan_results.append({
                    "ticker":  ticker,
                    "pattern": f"ERROR",
                    "direction": "—",
                    "confidence": 0,
                    "current": None,
                    "target":  None,
                    "pct_to_target": None,
                    "_df": None,
                    "_result": None,
                })
            prog.progress((i + 1) / len(tickers_to_scan))

        prog.empty()
        status.empty()
        st.session_state["pat_results"] = scan_results
        found = sum(1 for r in scan_results if r["pattern"] not in ("None", "NO DATA", "ERROR"))
        st.success(f"Scan complete — {found} patterns found in {len(scan_results)} tickers")

    # ── Results table ─────────────────────────────────────────
    if "pat_results" in st.session_state:
        results = st.session_state["pat_results"]

        # Filter by min confidence
        display = [r for r in results if r["confidence"] >= pat_min_conf or r["pattern"] in ("None", "NO DATA", "ERROR")]

        # Build summary DataFrame
        summary = []
        for r in display:
            summary.append({
                "Ticker":       r["ticker"],
                "Pattern":      r["pattern"],
                "Direction":    r["direction"],
                "Confidence %": r["confidence"] if r["confidence"] else 0,
                "Current":      r["current"],
                "Target":       r["target"],
                "% to Target":  r["pct_to_target"],
            })

        df_summary = pd.DataFrame(summary)

        def _style_pattern(val):
            if val == "Cup & Handle":
                return "color: #26a69a; font-weight: bold"
            elif val == "Head & Shoulders":
                return "color: #ef5350; font-weight: bold"
            elif val == "Inverse H&S":
                return "color: #42a5f5; font-weight: bold"
            return ""

        def _style_direction(val):
            if val == "BULLISH":
                return "color: #26a69a"
            elif val == "BEARISH":
                return "color: #ef5350"
            return ""

        def _style_pct(val):
            if val is None:
                return ""
            try:
                v = float(val)
                if v > 10:
                    return "color: #26a69a; font-weight: bold"
                elif v < -10:
                    return "color: #ef5350"
            except Exception:
                pass
            return ""

        styled = (
            df_summary.style
            .applymap(_style_pattern,   subset=["Pattern"])
            .applymap(_style_direction, subset=["Direction"])
            .applymap(_style_pct,       subset=["% to Target"])
            .format({
                "Current":      "${:.2f}",
                "Target":       "${:.2f}",
                "% to Target":  "{:.1f}%",
                "Confidence %": "{:.0f}%",
            }, na_rep="—")
        )

        st.markdown("### Scan Results")
        st.dataframe(
            styled,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Ticker":       st.column_config.TextColumn("Ticker",       width=80),
                "Pattern":      st.column_config.TextColumn("Pattern",      width=160),
                "Direction":    st.column_config.TextColumn("Direction",    width=100),
                "Confidence %": st.column_config.NumberColumn("Conf %",     width=80,  format="%.0f%%"),
                "Current":      st.column_config.NumberColumn("Current",    width=90,  format="$%.2f"),
                "Target":       st.column_config.NumberColumn("Target",     width=90,  format="$%.2f"),
                "% to Target":  st.column_config.NumberColumn("% to Target",width=100, format="%.1f%%"),
            },
            height=400,
        )

        # ── Chart Panel ───────────────────────────────────────
        st.markdown("---")
        st.markdown("### Pattern Chart")

        chartable = [r for r in results
                     if r["pattern"] not in ("None", "NO DATA", "ERROR")
                     and r["_df"] is not None
                     and r["_result"] is not None]

        if not chartable:
            st.info("No patterns detected. Try lowering the confidence threshold or scanning more tickers.")
        else:
            sel_pat_ticker = st.selectbox(
                "Select ticker to chart",
                [r["ticker"] for r in chartable],
                key="pat_chart_ticker",
            )
            sel_r = next((r for r in chartable if r["ticker"] == sel_pat_ticker), None)

            if sel_r:
                import plotly.graph_objects as go
                from plotly.subplots import make_subplots
                from skills.ta_engine import _ema

                df_chart = sel_r["_df"].iloc[-pat_lookback:].copy()
                result   = sel_r["_result"]
                pat      = result.get("pattern")
                offset   = len(sel_r["_df"]) - pat_lookback

                fig = make_subplots(
                    rows=2, cols=1, shared_xaxes=True,
                    row_heights=[0.80, 0.20],
                    vertical_spacing=0.03,
                )

                # Candlestick
                fig.add_trace(go.Candlestick(
                    x=df_chart.index,
                    open=df_chart["open"],  high=df_chart["high"],
                    low=df_chart["low"],    close=df_chart["close"],
                    name=sel_pat_ticker,
                    increasing_line_color="#26a69a",
                    decreasing_line_color="#ef5350",
                ), row=1, col=1)

                # EMA150
                ema150 = _ema(df_chart["close"], 150)
                fig.add_trace(go.Scatter(
                    x=df_chart.index, y=ema150,
                    line=dict(color="#5c9eff", width=1.2),
                    name="EMA150",
                ), row=1, col=1)

                # Volume
                colors = ["#26a69a" if c >= o else "#ef5350"
                          for c, o in zip(df_chart["close"], df_chart["open"])]
                fig.add_trace(go.Bar(
                    x=df_chart.index, y=df_chart["volume"],
                    marker_color=colors, name="Volume", showlegend=False,
                ), row=2, col=1)

                # ── Pattern overlays ──────────────────────────

                def idx_to_date(i):
                    """Convert absolute df index to chart date."""
                    local_i = i - offset
                    if 0 <= local_i < len(df_chart):
                        return df_chart.index[local_i]
                    return df_chart.index[-1]

                if pat == "Cup & Handle":
                    lri  = idx_to_date(result["left_rim_i"])
                    bi   = idx_to_date(result["bottom_i"])
                    rri  = idx_to_date(result["right_rim_i"])
                    hei  = idx_to_date(result["handle_end_i"])

                    # Cup shape
                    fig.add_trace(go.Scatter(
                        x=[lri, bi, rri],
                        y=[result["left_rim_val"], result["bottom_val"], result["right_rim_val"]],
                        mode="lines+markers",
                        line=dict(color="#ffd700", width=2, dash="dot"),
                        marker=dict(size=10, color="#ffd700"),
                        name="Cup",
                    ), row=1, col=1)

                    # Handle
                    fig.add_trace(go.Scatter(
                        x=[rri, hei],
                        y=[result["right_rim_val"], result["handle_low"]],
                        mode="lines+markers",
                        line=dict(color="#ffa726", width=2, dash="dot"),
                        marker=dict(size=8, color="#ffa726"),
                        name="Handle",
                    ), row=1, col=1)

                    # Target line
                    fig.add_hline(
                        y=result["target"],
                        line_dash="dash", line_color="#26a69a", line_width=1.5,
                        annotation_text=f"Target: ${result['target']:.2f} (+{result['pct_to_target']:.1f}%)",
                        annotation_position="right",
                        annotation_font_color="#26a69a",
                    )

                    # Resistance (rim level)
                    fig.add_hline(
                        y=result["right_rim_val"],
                        line_dash="dot", line_color="#ffd700", line_width=1,
                        annotation_text=f"Breakout: ${result['right_rim_val']:.2f}",
                        annotation_position="right",
                        annotation_font_color="#ffd700",
                    )

                elif pat in ("Head & Shoulders", "Inverse H&S"):
                    ls_date   = idx_to_date(result["ls_i"])
                    head_date = idx_to_date(result["head_i"])
                    rs_date   = idx_to_date(result["rs_i"])
                    t1_date   = idx_to_date(result["t1_i"])
                    t2_date   = idx_to_date(result["t2_i"])

                    color = "#ef5350" if pat == "Head & Shoulders" else "#42a5f5"

                    # H&S shape
                    fig.add_trace(go.Scatter(
                        x=[ls_date, t1_date, head_date, t2_date, rs_date],
                        y=[result["ls_val"], result["t1_val"],
                           result["head_val"], result["t2_val"], result["rs_val"]],
                        mode="lines+markers",
                        line=dict(color=color, width=2, dash="dot"),
                        marker=dict(size=10, color=color),
                        name=pat,
                    ), row=1, col=1)

                    # Neckline
                    fig.add_trace(go.Scatter(
                        x=[t1_date, t2_date],
                        y=[result["t1_val"], result["t2_val"]],
                        mode="lines",
                        line=dict(color="#ffd700", width=1.5),
                        name="Neckline",
                    ), row=1, col=1)

                    # Target line
                    tgt_color = "#26a69a" if pat == "Inverse H&S" else "#ef5350"
                    fig.add_hline(
                        y=result["target"],
                        line_dash="dash", line_color=tgt_color, line_width=1.5,
                        annotation_text=f"Target: ${result['target']:.2f} ({result['pct_to_target']:+.1f}%)",
                        annotation_position="right",
                        annotation_font_color=tgt_color,
                    )

                    # Neckline level
                    fig.add_hline(
                        y=result["neckline_at_rs"],
                        line_dash="dot", line_color="#ffd700", line_width=1,
                        annotation_text=f"Neckline: ${result['neckline_at_rs']:.2f}",
                        annotation_position="right",
                        annotation_font_color="#ffd700",
                    )

                fig.update_layout(
                    title=f"{sel_pat_ticker} — {pat} (Conf: {result['confidence']:.0f}%)",
                    xaxis_rangeslider_visible=False,
                    template="plotly_dark",
                    height=600,
                    margin=dict(l=10, r=160, t=40, b=10),
                    legend=dict(orientation="h", y=1.02),
                )
                fig.update_yaxes(title_text="Price", row=1, col=1)
                fig.update_yaxes(title_text="Vol",   row=2, col=1)

                st.plotly_chart(fig, use_container_width=True)

                # Metrics strip
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Pattern",     result.get("pattern", "—"))
                m2.metric("Direction",   result.get("direction", "—"))
                m3.metric("Confidence",  f"{result.get('confidence', 0):.0f}%")
                m4.metric("Target",      f"${result.get('target', 0):.2f}")
                m5.metric("% to Target", f"{result.get('pct_to_target', 0):+.1f}%")
