/* =========================================================================
   fw_prepare_processed.sas — Preparación idempotente de data processed
   Lee dataset maestro (raw), particiona train/oot por ventana de meses,
   genera base.sashdat y segNNN.sashdat por troncal/split.

   Usa casuser.cfg_troncales y casuser.cfg_segmentos como fuente de config
   (casuser es EXCLUSIVO para config).

   Datos operativos usan CASLIBs PATH-based:
     RAW  → data/raw/           (lectura del dataset maestro)
     PROC → data/processed/     (escritura de base/segNNN, subdirs=1)

   Requiere: %fw_path_processed y %_create_caslib, %_save_into_caslib,
             %_load_cas_data, %_drop_caslib (cas_utils.sas) ya cargados.

   Parámetro opcional:
     raw_table= nombre del archivo .sashdat sin extensión (default: mydataset)

   design.md §7.3 — Preparación idempotente:
     - Crea CASLIBs RAW y PROC.
     - Lee raw desde CASLIB RAW, NO desde casuser.
     - Sobrescribe outputs en CASLIB PROC.
     - Limpia tablas temporales CAS.
     - Loggea conteos (nobs) para auditoría mínima.
     - No deja tablas operativas en casuser.
   ========================================================================= */

%macro fw_prepare_processed(raw_table=mydataset);

  %put NOTE: ======================================================;
  %put NOTE: [fw_prepare_processed] INICIO — raw_table=&raw_table.;
  %put NOTE: ======================================================;

    /* -----------------------------------------------------------------
      0) Crear CASLIBs PATH-based para RAW y PROC
      ----------------------------------------------------------------- */
  %_create_caslib(
    cas_path     = &fw_root./data/raw,
    caslib_name  = RAW,
    lib_caslib   = RAW,
    global       = Y,
    cas_sess_name= conn,
    term_global_sess = 0,
    subdirs_flg  = 0
  );

  %_create_caslib(
    cas_path     = &fw_root./data/processed,
    caslib_name  = PROC,
    lib_caslib   = PROC,
    global       = Y,
    cas_sess_name= conn,
    term_global_sess = 0,
    subdirs_flg  = 1
  );

  /* Cargar raw desde CASLIB RAW */
  %_load_cas_data(
    caslib_name      = RAW,
    cas_sess_name    = conn,
    output_data_name = &raw_table.
  );

  /* -----------------------------------------------------------------
     1) Leer cfg_troncales para iterar (config vive en casuser)
     ----------------------------------------------------------------- */
  proc sql noprint;
    select count(*) into :_n_troncales trimmed from casuser.cfg_troncales;
  quit;

  %put NOTE: [fw_prepare_processed] Troncales a procesar: &_n_troncales.;

  data _null_;
    set casuser.cfg_troncales;
    call symputx(cats("_tr_id_",    _n_), troncal_id);
    call symputx(cats("_tr_byvar_", _n_), strip(byvar));
    call symputx(cats("_tr_tmin_",  _n_), train_min_mes);
    call symputx(cats("_tr_tmax_",  _n_), train_max_mes);
    call symputx(cats("_tr_omin_",  _n_), oot_min_mes);
    call symputx(cats("_tr_omax_",  _n_), oot_max_mes);
    call symputx(cats("_tr_vseg_",  _n_), strip(var_seg));
    call symputx(cats("_tr_nseg_",  _n_), n_segments);
  run;

  /* -----------------------------------------------------------------
     2) Iterar troncales
     ----------------------------------------------------------------- */
  %do _t = 1 %to &_n_troncales.;

    %let _tid   = &&_tr_id_&_t.;
    %let _byvar = &&_tr_byvar_&_t.;
    %let _tmin  = &&_tr_tmin_&_t.;
    %let _tmax  = &&_tr_tmax_&_t.;
    %let _omin  = &&_tr_omin_&_t.;
    %let _omax  = &&_tr_omax_&_t.;
    %let _vseg  = &&_tr_vseg_&_t.;
    %let _nseg  = &&_tr_nseg_&_t.;

    %put NOTE: -----------------------------------------------------;
    %put NOTE: [fw_prepare_processed] Troncal &_tid. (byvar=&_byvar.);
    %put NOTE: -----------------------------------------------------;

    /* ---- 2a) Crear base train y oot -------------------------------- */
    %do _s = 1 %to 2;
      %if &_s. = 1 %then %do;
        %let _split = train;
        %let _mmin  = &_tmin.;
        %let _mmax  = &_tmax.;
      %end;
      %else %do;
        %let _split = oot;
        %let _mmin  = &_omin.;
        %let _mmax  = &_omax.;
      %end;

      /* Resolver subruta relativa al CASLIB PROC */
      %fw_path_processed(outvar=_path_base, troncal_id=&_tid., split=&_split.);

      /* Crear tabla CAS filtrada (temporal en CASLIB RAW) */
      data RAW._tmp_base;
        set RAW.&raw_table.(where=(&_byvar. >= &_mmin. and &_byvar. <= &_mmax.));
      run;

      /* Contar obs para log */
      proc sql noprint;
        select count(*) into :_nobs_base trimmed from RAW._tmp_base;
      quit;
      %put NOTE: [fw_prepare_processed] &_path_base. => &_nobs_base. obs;

      %if &_nobs_base. = 0 %then %do;
        %put WARNING: [fw_prepare_processed] &_path_base. tiene 0 obs. Se crea vacío.;
      %end;

      /* Guardar como .sashdat en CASLIB PROC (subruta con subdirs) */
      %_save_into_caslib(
        m_cas_sess_name = conn,
        m_input_caslib  = RAW,
        m_input_data    = _tmp_base,
        m_output_caslib = PROC,
        m_subdir_data   = %sysfunc(tranwrd(&_path_base., .sashdat, ))
      );

      /* ---- 2b) Segmentos (si aplica) ------------------------------- */
      %if %superq(_vseg) ne and &_nseg. > 0 %then %do;
        %do _sg = 1 %to &_nseg.;

          %fw_path_processed(outvar=_path_seg, troncal_id=&_tid., split=&_split., seg_id=&_sg.);

          data RAW._tmp_seg;
            set RAW._tmp_base(where=(&_vseg. = &_sg.));
          run;

          proc sql noprint;
            select count(*) into :_nobs_seg trimmed from RAW._tmp_seg;
          quit;
          %put NOTE: [fw_prepare_processed] &_path_seg. => &_nobs_seg. obs;

          %_save_into_caslib(
            m_cas_sess_name = conn,
            m_input_caslib  = RAW,
            m_input_data    = _tmp_seg,
            m_output_caslib = PROC,
            m_subdir_data   = %sysfunc(tranwrd(&_path_seg., .sashdat, ))
          );

          /* Limpiar temporal */
          proc cas;
            table.dropTable / caslib="RAW" name="_tmp_seg" quiet=true;
          quit;

        %end; /* segmentos */
      %end;

      /* Limpiar base temporal */
      proc cas;
        table.dropTable / caslib="RAW" name="_tmp_base" quiet=true;
      quit;

    %end; /* splits */

  %end; /* troncales */

  /* -----------------------------------------------------------------
     3) Cleanup: CASLIBs y macrovariables temporales
        Los .sashdat persisten en disco; solo liberamos memoria CAS.
     ----------------------------------------------------------------- */
  %_drop_caslib(caslib_name=RAW,  cas_sess_name=conn, del_prom_tables=1);
  %_drop_caslib(caslib_name=PROC, cas_sess_name=conn, del_prom_tables=1);

  %do _t = 1 %to &_n_troncales.;
    %symdel _tr_id_&_t. _tr_byvar_&_t. _tr_tmin_&_t. _tr_tmax_&_t.
            _tr_omin_&_t. _tr_omax_&_t. _tr_vseg_&_t. _tr_nseg_&_t. / nowarn;
  %end;
  %symdel _n_troncales / nowarn;

  %put NOTE: ======================================================;
  %put NOTE: [fw_prepare_processed] FIN;
  %put NOTE: ======================================================;

%mend fw_prepare_processed;
