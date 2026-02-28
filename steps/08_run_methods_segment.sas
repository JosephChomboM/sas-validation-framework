/* =========================================================================
   steps/08_run_methods_segment.sas — Step 8: Ejecución subflow segmento
   Define macro para ejecutar métodos sobre contexto segmento promovido.
   ========================================================================= */

/* Cargar dispatch */
%include "&fw_root./src/dispatch/run_module.sas";
%include "&fw_root./src/dispatch/run_method.sas";

%macro run_methods_segment_context(run_id=);

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

  %macro _run_methods_for_context(_troncal=, _split=, _seg=);
    %if &metodo_1_enabled_seg. = 1 and %superq(metodo_1_modules_seg) ne %then
      %run_method(method_modules=&metodo_1_modules_seg., troncal_id=&_troncal., split=&_split., seg_id=&_seg., run_id=&run_id.);

    %if &metodo_2_enabled_seg. = 1 and %superq(metodo_2_modules_seg) ne %then
      %run_method(method_modules=&metodo_2_modules_seg., troncal_id=&_troncal., split=&_split., seg_id=&_seg., run_id=&run_id.);

    %if &metodo_3_enabled_seg. = 1 and %superq(metodo_3_modules_seg) ne %then
      %run_method(method_modules=&metodo_3_modules_seg., troncal_id=&_troncal., split=&_split., seg_id=&_seg., run_id=&run_id.);
  %mend _run_methods_for_context;

  %if %upcase(&ctx_segment_mode.) = ALL %then %do;
    proc sql noprint;
      select count(*) into :_n_tr_seg trimmed from casuser.cfg_troncales;
    quit;

    data _null_;
      set casuser.cfg_troncales;
      call symputx(cats('_seg_tid_', _n_), troncal_id);
      call symputx(cats('_seg_nsg_', _n_), n_segments);
    run;

    %do _i = 1 %to &_n_tr_seg.;
      %let _tid = &&_seg_tid_&_i.;
      %let _nsg = &&_seg_nsg_&_i.;

      %if &_nsg. > 0 %then %do;
        %do _sg = 1 %to &_nsg.;
          %if %superq(_sp1) ne %then %_run_methods_for_context(_troncal=&_tid., _split=&_sp1., _seg=&_sg.);
          %if %superq(_sp2) ne %then %_run_methods_for_context(_troncal=&_tid., _split=&_sp2., _seg=&_sg.);
        %end;
      %end;
    %end;
  %end;
  %else %do;
    %let _tid = &ctx_segment_troncal_id.;

    %if %upcase(&ctx_segment_seg_id.) = ALL %then %do;
      proc sql noprint;
        select n_segments into :_nsg_one trimmed
        from casuser.cfg_troncales
        where troncal_id = &_tid.;
      quit;

      %do _sg = 1 %to &_nsg_one.;
        %if %superq(_sp1) ne %then %_run_methods_for_context(_troncal=&_tid., _split=&_sp1., _seg=&_sg.);
        %if %superq(_sp2) ne %then %_run_methods_for_context(_troncal=&_tid., _split=&_sp2., _seg=&_sg.);
      %end;
    %end;
    %else %do;
      %if %superq(_sp1) ne %then %_run_methods_for_context(_troncal=&_tid., _split=&_sp1., _seg=&ctx_segment_seg_id.);
      %if %superq(_sp2) ne %then %_run_methods_for_context(_troncal=&_tid., _split=&_sp2., _seg=&ctx_segment_seg_id.);
    %end;
  %end;

%mend run_methods_segment_context;

%if &partition_enabled. = 1 %then %do;
  %run_methods_segment_context(run_id=&run_id.);
  %put NOTE: [step-08] Subflow segmento ejecutado.;
%end;
%else %do;
  %put WARNING: [step-08] partition_enabled=0; se omite ejecución de segmento.;
%end;
