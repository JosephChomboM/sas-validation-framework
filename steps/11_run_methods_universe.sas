/* =========================================================================
   steps/11_run_methods_universe.sas — Step 11: Ejecución subflow universo
   Define macro para ejecutar métodos sobre contexto universo promovido.
   ========================================================================= */

/* Cargar dispatch */
%include "&fw_root./src/dispatch/run_module.sas";
%include "&fw_root./src/dispatch/run_method.sas";

%macro run_methods_universe_context(run_id=);

  %local _sp1 _sp2;
  %if %upcase(&ctx_universe_split.) = TRAIN %then %do;
    %let _sp1 = train; %let _sp2 = ;
  %end;
  %else %if %upcase(&ctx_universe_split.) = OOT %then %do;
    %let _sp1 = oot; %let _sp2 = ;
  %end;
  %else %do;
    %let _sp1 = train; %let _sp2 = oot;
  %end;

  %macro _run_methods_for_unv(_troncal=, _split=);
    %if &metodo_1_enabled_unv. = 1 and %superq(metodo_1_modules_unv) ne %then
      %run_method(method_modules=&metodo_1_modules_unv., troncal_id=&_troncal., split=&_split., seg_id=, run_id=&run_id.);

    %if &metodo_2_enabled_unv. = 1 and %superq(metodo_2_modules_unv) ne %then
      %run_method(method_modules=&metodo_2_modules_unv., troncal_id=&_troncal., split=&_split., seg_id=, run_id=&run_id.);

    %if &metodo_3_enabled_unv. = 1 and %superq(metodo_3_modules_unv) ne %then
      %run_method(method_modules=&metodo_3_modules_unv., troncal_id=&_troncal., split=&_split., seg_id=, run_id=&run_id.);
  %mend _run_methods_for_unv;

  %if %upcase(&ctx_universe_mode.) = ALL %then %do;
    proc sql noprint;
      select count(*) into :_n_tr_unv trimmed from casuser.cfg_troncales;
    quit;

    data _null_;
      set casuser.cfg_troncales;
      call symputx(cats('_unv_tid_', _n_), troncal_id);
    run;

    %do _i = 1 %to &_n_tr_unv.;
      %let _tid = &&_unv_tid_&_i.;
      %if %superq(_sp1) ne %then %_run_methods_for_unv(_troncal=&_tid., _split=&_sp1.);
      %if %superq(_sp2) ne %then %_run_methods_for_unv(_troncal=&_tid., _split=&_sp2.);
    %end;
  %end;
  %else %do;
    %let _tid = &ctx_universe_troncal_id.;
    %if %superq(_sp1) ne %then %_run_methods_for_unv(_troncal=&_tid., _split=&_sp1.);
    %if %superq(_sp2) ne %then %_run_methods_for_unv(_troncal=&_tid., _split=&_sp2.);
  %end;

%mend run_methods_universe_context;

%if &partition_enabled. = 1 %then %do;
  %run_methods_universe_context(run_id=&run_id.);
  %put NOTE: [step-11] Subflow universo ejecutado.;
%end;
%else %do;
  %put WARNING: [step-11] partition_enabled=0; se omite ejecución de universo.;
%end;

/* Cleanup final del framework */
%_drop_caslib(caslib_name=OUT, cas_sess_name=conn, del_prom_tables=1);
%_drop_caslib(caslib_name=RAW,       cas_sess_name=conn, del_prom_tables=1);
%_drop_caslib(caslib_name=PROC,      cas_sess_name=conn, del_prom_tables=1);

%put NOTE: ======================================================;
%put NOTE: [step-11] Run &run_id. completado.;
%put NOTE: [step-11] Outputs en: &fw_root./outputs/runs/&run_id./;
%put NOTE: ======================================================;

cas conn terminate;
