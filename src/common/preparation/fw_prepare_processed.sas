/* =========================================================================
   fw_prepare_processed.sas — Preparación idempotente de data processed
   Lee dataset maestro (raw), particiona train/oot por ventana de meses,
   genera base.sashdat y segNNN.sashdat por troncal/split.

   Usa casuser.cfg_troncales y casuser.cfg_segmentos como fuente de config.
   Requiere: %fw_path_processed (src/common/fw_paths.sas) ya cargado.

   Parámetro opcional:
     raw_table= nombre CAS del dataset raw (default: mydataset)

   design.md §7.2 — Preparación idempotente:
     - Sobrescribe outputs processed de manera controlada.
     - Limpia tablas temporales CAS.
     - Loggea conteos (nobs) para auditoría mínima.
   ========================================================================= */

%macro fw_prepare_processed(raw_table=mydataset);

  %put NOTE: ======================================================;
  %put NOTE: [fw_prepare_processed] INICIO — raw_table=&raw_table.;
  %put NOTE: ======================================================;

  /* -----------------------------------------------------------------
     0) Asegurar que la tabla raw existe en casuser
     ----------------------------------------------------------------- */
  %if not %sysfunc(exist(casuser.&raw_table.)) %then %do;
    %put ERROR: [fw_prepare_processed] casuser.&raw_table. no existe. Cargue el raw primero.;
    %abort cancel;
  %end;

  /* -----------------------------------------------------------------
     1) Leer cfg_troncales en vista local para iterar
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

      /* Resolver ruta base */
      %fw_path_processed(outvar=_path_base, troncal_id=&_tid., split=&_split.);

      /* Crear tabla CAS filtrada (base = universo del split) */
      data casuser._tmp_base;
        set casuser.&raw_table.(where=(&_byvar. >= &_mmin. and &_byvar. <= &_mmax.));
      run;

      /* Contar obs para log */
      proc sql noprint;
        select count(*) into :_nobs_base trimmed from casuser._tmp_base;
      quit;
      %put NOTE: [fw_prepare_processed] &_path_base. => &_nobs_base. obs;

      %if &_nobs_base. = 0 %then %do;
        %put WARNING: [fw_prepare_processed] &_path_base. tiene 0 obs. Se crea vacío.;
      %end;

      /* Guardar como .sashdat en processed */
      proc casutil;
        save casdata="_tmp_base" incaslib="casuser"
             casout="&_path_base." outcaslib="casuser" replace;
      run;

      /* ---- 2b) Segmentos (si aplica) ------------------------------- */
      %if %superq(_vseg) ne and &_nseg. > 0 %then %do;
        %do _sg = 1 %to &_nseg.;

          %fw_path_processed(outvar=_path_seg, troncal_id=&_tid., split=&_split., seg_id=&_sg.);

          data casuser._tmp_seg;
            set casuser._tmp_base(where=(&_vseg. = &_sg.));
          run;

          proc sql noprint;
            select count(*) into :_nobs_seg trimmed from casuser._tmp_seg;
          quit;
          %put NOTE: [fw_prepare_processed] &_path_seg. => &_nobs_seg. obs;

          proc casutil;
            save casdata="_tmp_seg" incaslib="casuser"
                 casout="&_path_seg." outcaslib="casuser" replace;
          run;

          /* Limpiar temporal */
          proc casutil;
            droptable casdata="_tmp_seg" incaslib="casuser" quiet;
          run;

        %end; /* segmentos */
      %end;

      /* Limpiar base temporal */
      proc casutil;
        droptable casdata="_tmp_base" incaslib="casuser" quiet;
      run;

    %end; /* splits */

  %end; /* troncales */

  /* -----------------------------------------------------------------
     3) Limpieza de macrovariables temporales
     ----------------------------------------------------------------- */
  %do _t = 1 %to &_n_troncales.;
    %symdel _tr_id_&_t. _tr_byvar_&_t. _tr_tmin_&_t. _tr_tmax_&_t.
            _tr_omin_&_t. _tr_omax_&_t. _tr_vseg_&_t. _tr_nseg_&_t. / nowarn;
  %end;
  %symdel _n_troncales / nowarn;

  %put NOTE: ======================================================;
  %put NOTE: [fw_prepare_processed] FIN;
  %put NOTE: ======================================================;

%mend fw_prepare_processed;
