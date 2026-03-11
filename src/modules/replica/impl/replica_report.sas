/* =========================================================================
replica_report.sas - Reportes HTML + Excel + JPEG para Replica

Genera:
<report_path>/<prefix>_train.html  - reporte TRAIN
<report_path>/<prefix>_oot.html    - reporte OOT
<report_path>/<prefix>.xlsx        - Excel multi-hoja TRAIN/OOT
<images_path>/<prefix>_*.jpeg      - graficos de apoyo

Replica usa Pattern B:
- copia CAS -> work con keep= y filtro byvar <= def_cld
- ejecuta computo work-only
- genera tablas y graficos por split
========================================================================= */

%macro _replica_render_block(data=, alias=, data_label=, target=, vars_num=,
    ponderada=1, groups=10, time_var=, control_var=, file_prefix=);

    %local _has_control _has_dw;
    %let _has_control=0;
    %let _has_dw=0;

    %if %length(%superq(control_var)) > 0 %then %let _has_control=1;
    %if %length(%superq(time_var)) > 0 %then %let _has_dw=1;

    %_replica_compute(data=&data., alias=&alias., target=&target.,
        lista_var=&vars_num., ponderada=&ponderada., hits=1,
        groups=&groups., time_var=&time_var., control_var=&control_var.);

    title "Replica del Modelo - &data_label.";
    proc print data=work.&alias._nobs noobs;
    run;
    title;

    title "Perfil de Respuesta - &data_label.";
    proc print data=work.&alias._responseprofile noobs;
    run;
    title;

    title "Ajuste del Modelo - &data_label.";
    proc print data=work.&alias._fitstatistics noobs;
    run;
    title;

    title "Betas y Pesos - &data_label.";
    proc print data=work.&alias._betas_report noobs label;
        label Variable="Variable"
            Estimate="Estimate"
            StdErr="StdErr"
            ProbChiSq="ProbChiSq"
            peso="Peso";
    run;
    title;

    title "Asociacion del Modelo - &data_label.";
    proc print data=work.&alias._stats noobs;
    run;
    title;

    %if &_has_control.=1 %then %do;
        title "Control vs Probabilidad Replicada - &data_label.";
        proc print data=work.&alias._control noobs label;
            label control_var="Variable control"
                n_obs="Observaciones"
                prom_y_est="Promedio y_est"
                prom_control="Promedio control"
                corr_y_est="Correlacion"
                mae="MAE"
                rmse="RMSE";
        run;
        title;
    %end;

    title "Factor de Inflacion de Varianza (VIF) - &data_label.";
    proc print data=work.&alias._vif noobs label;
        var Dependent Variable Estimate StdErr tValue Probt;
        var VarianceInflation / style={background=vif_fmt.};
        where Variable ne 'Intercept';
        label Dependent="RD"
            Estimate="Estimado"
            StdErr="Error Std"
            tValue="T-value"
            Probt="Prob T"
            VarianceInflation="VIF";
    run;
    title;

    title "Normalidad de Residuos - &data_label.";
    proc print data=work.&alias._normality noobs;
    run;
    title;

    title "Cortes para Test de Levene - &data_label.";
    proc print data=work.&alias._cuts noobs;
    run;
    title;

    title "Supuesto de Homocedasticidad (Levene) - &data_label.";
    proc print data=work.&alias._levene noobs label;
        where Source ne 'Error';
        var Dependent Method Source SS MS FValue;
        var ProbF / style={background=levene_fmt.};
        label Method="Metodo"
            Source="Fuente"
            SS="Suma Cuadr."
            MS="Media Cuadr."
            FValue="F"
            ProbF="ProbF";
    run;
    title;

    %if &_has_dw.=1 %then %do;
        title "Autocorrelacion de Residuos (Durbin-Watson) - &data_label.";
        proc print data=work.&alias._dw noobs label;
            where Order=1;
            var Order;
            var DW / style={background=dw_fmt.};
            var ProbDW ProbDWNeg;
            label Order="Orden"
                DW="Durbin-Watson"
                ProbDW="P-value Positivo"
                ProbDWNeg="P-value Negativo";
            format DW 8.4 ProbDW ProbDWNeg pvalue6.4;
        run;
        title;
    %end;

    ods graphics / imagename="&file_prefix._%lowcase(&data_label.)_resid"
        imagefmt=jpeg;
    title "Distribucion de Residuos - &data_label.";
    proc sgplot data=work.&alias._residuals;
        histogram residuals / fillattrs=(color=steelblue transparency=0.25);
        density residuals / type=kernel lineattrs=(color=navy thickness=2);
        refline 0 / axis=x lineattrs=(color=gray pattern=shortdash);
        xaxis label="Residual";
        yaxis label="Frecuencia";
    run;
    title;

    ods graphics / imagename="&file_prefix._%lowcase(&data_label.)_predres"
        imagefmt=jpeg;
    title "Predicho vs Residual - &data_label.";
    proc sgplot data=work.&alias._residuals;
        scatter x=predicted y=residuals / transparency=0.3
            markerattrs=(color=cx1F77B4 symbol=CircleFilled size=7);
        refline 0 / axis=y lineattrs=(color=gray pattern=shortdash);
        xaxis label="Predicho";
        yaxis label="Residual";
    run;
    title;

    %if &_has_control.=1 %then %do;
        ods graphics / imagename=
            "&file_prefix._%lowcase(&data_label.)_control" imagefmt=jpeg;
        title "y_est vs &control_var. - &data_label.";
        proc sgplot data=work.&alias._out;
            scatter x=&control_var. y=y_est / transparency=0.3
                markerattrs=(color=cxD62728 symbol=CircleFilled size=7);
            lineparm x=0 y=0 slope=1 /
                lineattrs=(color=gray pattern=shortdash);
            xaxis label="&control_var.";
            yaxis label="y_est";
        run;
        title;
    %end;

%mend _replica_render_block;

%macro _replica_report(input_caslib=, train_table=, oot_table=, byvar=,
    target=, vars_num=, time_var=, control_var=, def_cld=0, ponderada=1,
    groups=10, run_id=, report_path=, images_path=, file_prefix=);

    %local _dir_rc _keep_vars;

    %put NOTE: [replica_report] Generando reportes...;
    %put NOTE: [replica_report] report_path=&report_path.;
    %put NOTE: [replica_report] images_path=&images_path.;
    %put NOTE: [replica_report] file_prefix=&file_prefix.;
    %put NOTE: [replica_report] def_cld=&def_cld. control_var=&control_var.;

    %let _keep_vars=&vars_num. &target. &byvar.;
    %if %length(%superq(time_var)) > 0 %then
        %let _keep_vars=&_keep_vars. &time_var.;
    %if %length(%superq(control_var)) > 0 %then
        %let _keep_vars=&_keep_vars. &control_var.;
    %let _keep_vars=%sysfunc(compbl(&_keep_vars.));

    /* ---- Crear directorios si no existen ------------------------------ */
    %if %index(%upcase(&report_path.), EXPERIMENTS)=0 %then %do;
        %let _dir_rc=%sysfunc(dcreate(METOD5.2.1, &fw_root./outputs/runs/&run_id./reports));
        %let _dir_rc=%sysfunc(dcreate(METOD5.2.1, &fw_root./outputs/runs/&run_id./images));
    %end;

    /* ---- CAS -> work con default cerrado ------------------------------ */
    %if %length(%superq(byvar)) > 0 and %length(%superq(def_cld)) > 0 and
        &def_cld. ne 0 %then %do;

        data work._rep_train;
            set &input_caslib..&train_table.(keep=&_keep_vars.);
            where &byvar. <= &def_cld.;
        run;

        data work._rep_oot;
            set &input_caslib..&oot_table.(keep=&_keep_vars.);
            where &byvar. <= &def_cld.;
        run;

    %end;
    %else %do;

        data work._rep_train;
            set &input_caslib..&train_table.(keep=&_keep_vars.);
        run;

        data work._rep_oot;
            set &input_caslib..&oot_table.(keep=&_keep_vars.);
        run;

    %end;

    ods graphics on;
    ods listing gpath="&images_path.";

    /* ==================================================================
       TRAIN
       ================================================================== */
    ods html5 file="&report_path./&file_prefix._train.html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="TRAIN_Replica" sheet_interval="none"
        embedded_titles="yes");

    %_replica_render_block(data=work._rep_train, alias=_rep_trn,
        data_label=TRAIN, target=&target., vars_num=&vars_num.,
        ponderada=&ponderada., groups=&groups., time_var=&time_var.,
        control_var=&control_var., file_prefix=&file_prefix.);

    ods html5 close;
    ods graphics / reset=all;

    /* ==================================================================
       OOT
       ================================================================== */
    ods html5 file="&report_path./&file_prefix._oot.html"
        options(bitmap_mode="inline");
    ods excel options(sheet_name="OOT_Replica" sheet_interval="now"
        embedded_titles="yes");

    %_replica_render_block(data=work._rep_oot, alias=_rep_ootx,
        data_label=OOT, target=&target., vars_num=&vars_num.,
        ponderada=&ponderada., groups=&groups., time_var=&time_var.,
        control_var=&control_var., file_prefix=&file_prefix.);

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    proc datasets library=work nolist nowarn;
        delete _rep_:;
    quit;

    %put NOTE: [replica_report] HTML TRAIN =>
        &report_path./&file_prefix._train.html;
    %put NOTE: [replica_report] HTML OOT =>
        &report_path./&file_prefix._oot.html;
    %put NOTE: [replica_report] Excel => &report_path./&file_prefix..xlsx;
    %put NOTE: [replica_report] Images => &images_path./;

%mend _replica_report;
