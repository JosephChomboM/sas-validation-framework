/* =========================================================================
monotonicidad_report.sas - Reportes HTML + Excel + JPEG para METOD7

Genera:
<report_path>/<prefix>_train.html  - TRAIN (inline)
<report_path>/<prefix>_oot.html    - OOT (inline)
<report_path>/<prefix>.xlsx        - Excel TRAIN + OOT
<images_path>/<prefix>_*.jpeg      - graficos independientes

Regla clave de negocio:
- Usa default cerrado: filtra TRAIN y OOT con byvar <= def_cld.
========================================================================= */
%macro _monotonicidad_report(input_caslib=, train_table=, oot_table=, byvar=,
    score_var=, target_var=, def_cld=0, groups=5, report_path=, images_path=,
    file_prefix=);

    %put NOTE: [monotonicidad_report] Generando reportes...;
    %put NOTE: [monotonicidad_report] byvar=&byvar. score=&score_var.
        target=&target_var. def_cld=&def_cld.;

    /* ---- Copiar desde CAS a work (Pattern B) -------------------------- */
    data work._mono_train_raw;
        set &input_caslib..&train_table.;
    run;

    data work._mono_oot_raw;
        set &input_caslib..&oot_table.;
    run;

    /* ---- Filtrar default cerrado -------------------------------------- */
    %if %length(%superq(byvar)) > 0 and %length(%superq(def_cld)) > 0 and
        &def_cld. ne 0 %then %do;

        data work._mono_train;
            set work._mono_train_raw;
            where &byvar. <= &def_cld.;
        run;

        data work._mono_oot;
            set work._mono_oot_raw;
            where &byvar. <= &def_cld.;
        run;
    %end;
    %else %do;
        data work._mono_train;
            set work._mono_train_raw;
        run;

        data work._mono_oot;
            set work._mono_oot_raw;
        run;
    %end;

    /* ---- Crear directorios METOD7 si no existen ----------------------- */
    %local _dir_rc;
    %let _dir_rc=%sysfunc(dcreate(METOD7, &report_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &report_path.));
    %let _dir_rc=%sysfunc(dcreate(METOD7, &images_path./../));
    %let _dir_rc=%sysfunc(dcreate(., &images_path.));

    /* ==================================================================
       TRAIN
       ================================================================== */
    ods graphics on;
    ods listing gpath="&images_path.";

    ods html5 file="&report_path./&file_prefix._train.html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="TRAIN_Monotonicidad" sheet_interval="none"
        embedded_titles="yes");
    ods graphics / imagename="&file_prefix._trn_mono" imagefmt=jpeg;

    %_mono_build_report(tablain=work._mono_train, score_var=&score_var.,
        target_var=&target_var., groups=&groups., use_existing_cuts=0,
        cuts_table=work._mono_cortes, out_table=work._mono_report_train);
    %_mono_plot_and_print(report_table=work._mono_report_train,
        score_var=&score_var., target_var=&target_var., data_type=TRAIN);

    ods html5 close;
    ods graphics / reset=all;

    /* ==================================================================
       OOT (reusa cortes de TRAIN)
       ================================================================== */
    ods html5 file="&report_path./&file_prefix._oot.html"
        options(bitmap_mode="inline");
    ods excel options(sheet_name="OOT_Monotonicidad" sheet_interval="now"
        embedded_titles="yes");
    ods graphics / imagename="&file_prefix._oot_mono" imagefmt=jpeg;

    %_mono_build_report(tablain=work._mono_oot, score_var=&score_var.,
        target_var=&target_var., groups=&groups., use_existing_cuts=1,
        cuts_table=work._mono_cortes, out_table=work._mono_report_oot);
    %_mono_plot_and_print(report_table=work._mono_report_oot,
        score_var=&score_var., target_var=&target_var., data_type=OOT);

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    /* ---- Cleanup work -------------------------------------------------- */
    proc datasets library=work nolist nowarn;
        delete _mono_:;
    quit;

    %put NOTE: [monotonicidad_report] HTML TRAIN=>
        &report_path./&file_prefix._train.html;
    %put NOTE: [monotonicidad_report] HTML OOT=>
        &report_path./&file_prefix._oot.html;
    %put NOTE: [monotonicidad_report] Excel=>
        &report_path./&file_prefix..xlsx;
    %put NOTE: [monotonicidad_report] Images=> &images_path./;

%mend _monotonicidad_report;

