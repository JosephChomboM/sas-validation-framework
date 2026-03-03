/* =========================================================================
   steps/methods/metod_4/step_correlacion.sas
   Step de módulo: Correlación (Método 4)

   Flujo:
     1) Configuración propia del módulo (corr_mode, corr_custom_vars)
     2) Crear CASLIBs PROC + OUT
     3) Bloque SEGMENTO — iterar según ctx_segment_* (Step 06)
     4) Bloque UNIVERSO — iterar según ctx_universe_* (Step 09)
     5) Cleanup CASLIBs

   Dependencias:
     - Macro vars de Step 06 (ctx_segment_mode, ctx_segment_troncal_id,
       ctx_segment_split, ctx_segment_seg_id)
     - Macro vars de Step 09 (ctx_universe_mode, ctx_universe_troncal_id,
       ctx_universe_split)
     - casuser.cfg_troncales / cfg_segmentos (promovidas en Step 02)
     - &fw_root., &run_id (Steps 01 y 02)

   Cada step es independiente: carga sus propias dependencias.
   ========================================================================= */

/* ---- Dependencias ----------------------------------------------------- */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";

/* ---- CONFIGURACIÓN DEL MÓDULO (editar aquí) --------------------------- */

/* corr_mode:
     AUTO   → usa variables de cfg_segmentos/cfg_troncales (num_list/num_unv)
              Outputs van a reports/ y tables/ (validación estándar).
     CUSTOM → usa corr_custom_vars (lista manual de variables numéricas)
              Outputs van a experiments/ (análisis exploratorio).           */
%let corr_mode        = AUTO;
%let corr_custom_vars = ;

%put NOTE: [step_correlacion] corr_mode=&corr_mode.;

/* ---- EJECUCIÓN -------------------------------------------------------- */
%macro _step_correlacion;

  /* ---- 1) Crear CASLIBs PROC + OUT ----------------------------------- */
  %_create_caslib(
    cas_path     = &fw_root./data/processed,
    caslib_name  = PROC,
    lib_caslib   = PROC,
    global       = Y,
    cas_sess_name= conn,
    term_global_sess = 0,
    subdirs_flg  = 1
  );
  %_create_caslib(
    cas_path     = &fw_root./outputs/runs/&run_id.,
    caslib_name  = OUT,
    lib_caslib   = OUT,
    global       = Y,
    cas_sess_name= conn,
    term_global_sess = 0,
    subdirs_flg  = 1
  );

  /* ==================================================================
     2) BLOQUE SEGMENTO — usa ctx_segment_* de Step 06
     ================================================================== */
  %local _sp1 _sp2;

  %if %upcase(&ctx_segment_split.) = TRAIN %then %do;
    %let _sp1 = train; %let _sp2 = ;
  %end;
  %else %if %upcase(&ctx_segment_split.) = OOT %then %do;
    %let _sp1 = oot; %let _sp2 = ;
  %end;
  %else %do;
    %let _sp1 = train; %let _sp2 = oot;
  %end;

  %put NOTE: [step_correlacion] === SEGMENTO: mode=&ctx_segment_mode. split=&ctx_segment_split. ===;

  %if %upcase(&ctx_segment_mode.) = ALL %then %do;

    proc sql noprint;
      select count(*) into :_n_tr trimmed from casuser.cfg_troncales;
    quit;
    data _null_;
      set casuser.cfg_troncales;
      call symputx(cats('_cr_tid_', _n_), troncal_id);
      call symputx(cats('_cr_nsg_', _n_), n_segments);
    run;

    %do _i = 1 %to &_n_tr.;
      %let _tid = &&_cr_tid_&_i.;
      %let _nsg = &&_cr_nsg_&_i.;

      %if &_nsg. > 0 %then %do;
        %do _sg = 1 %to &_nsg.;
          %if %superq(_sp1) ne %then
            %run_module(module=correlacion, troncal_id=&_tid., split=&_sp1., seg_id=&_sg., run_id=&run_id.);
          %if %superq(_sp2) ne %then
            %run_module(module=correlacion, troncal_id=&_tid., split=&_sp2., seg_id=&_sg., run_id=&run_id.);
        %end;
      %end;
    %end;

  %end;
  %else %do;
    /* mode=ONE: troncal y seg_id específicos */
    %let _tid = &ctx_segment_troncal_id.;

    %if %upcase(&ctx_segment_seg_id.) = ALL %then %do;
      proc sql noprint;
        select n_segments into :_nsg_one trimmed
        from casuser.cfg_troncales where troncal_id = &_tid.;
      quit;

      %do _sg = 1 %to &_nsg_one.;
        %if %superq(_sp1) ne %then
          %run_module(module=correlacion, troncal_id=&_tid., split=&_sp1., seg_id=&_sg., run_id=&run_id.);
        %if %superq(_sp2) ne %then
          %run_module(module=correlacion, troncal_id=&_tid., split=&_sp2., seg_id=&_sg., run_id=&run_id.);
      %end;
    %end;
    %else %do;
      %if %superq(_sp1) ne %then
        %run_module(module=correlacion, troncal_id=&_tid., split=&_sp1., seg_id=&ctx_segment_seg_id., run_id=&run_id.);
      %if %superq(_sp2) ne %then
        %run_module(module=correlacion, troncal_id=&_tid., split=&_sp2., seg_id=&ctx_segment_seg_id., run_id=&run_id.);
    %end;
  %end;

  /* ==================================================================
     3) BLOQUE UNIVERSO — usa ctx_universe_* de Step 09
     ================================================================== */
  %if %upcase(&ctx_universe_split.) = TRAIN %then %do;
    %let _sp1 = train; %let _sp2 = ;
  %end;
  %else %if %upcase(&ctx_universe_split.) = OOT %then %do;
    %let _sp1 = oot; %let _sp2 = ;
  %end;
  %else %do;
    %let _sp1 = train; %let _sp2 = oot;
  %end;

  %put NOTE: [step_correlacion] === UNIVERSO: mode=&ctx_universe_mode. split=&ctx_universe_split. ===;

  %if %upcase(&ctx_universe_mode.) = ALL %then %do;

    proc sql noprint;
      select count(*) into :_n_tr_u trimmed from casuser.cfg_troncales;
    quit;
    data _null_;
      set casuser.cfg_troncales;
      call symputx(cats('_cr_utid_', _n_), troncal_id);
    run;

    %do _i = 1 %to &_n_tr_u.;
      %let _tid = &&_cr_utid_&_i.;
      %if %superq(_sp1) ne %then
        %run_module(module=correlacion, troncal_id=&_tid., split=&_sp1., seg_id=, run_id=&run_id.);
      %if %superq(_sp2) ne %then
        %run_module(module=correlacion, troncal_id=&_tid., split=&_sp2., seg_id=, run_id=&run_id.);
    %end;

  %end;
  %else %do;
    %let _tid = &ctx_universe_troncal_id.;
    %if %superq(_sp1) ne %then
      %run_module(module=correlacion, troncal_id=&_tid., split=&_sp1., seg_id=, run_id=&run_id.);
    %if %superq(_sp2) ne %then
      %run_module(module=correlacion, troncal_id=&_tid., split=&_sp2., seg_id=, run_id=&run_id.);
  %end;

  /* ---- 4) Cleanup CASLIBs --------------------------------------------- */
  %_drop_caslib(caslib_name=OUT,  cas_sess_name=conn, del_prom_tables=1);
  %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

  %put NOTE: ======================================================;
  %put NOTE: [step_correlacion] Completado (mode=&corr_mode.);
  %put NOTE: ======================================================;

%mend _step_correlacion;
%_step_correlacion;
