/* =========================================================================
   run_method.sas — Dispatch: ejecuta un método (conjunto de módulos)
   Un "método" agrupa N módulos a ejecutar sobre el mismo contexto.

   Parámetros:
     method_modules  = lista de módulos separados por espacio (ej. "gini psi")
     troncal_id      = id numérico de troncal
     split           = train | oot
     seg_id          = id numérico de segmento (vacío = universo/base)
     run_id          = identificador del run actual
   ========================================================================= */

%macro run_method(method_modules=, troncal_id=, split=, seg_id=, run_id=);

  %let _n_mod = %sysfunc(countw(&method_modules., %str( )));

  %do _m = 1 %to &_n_mod.;
    %let _mod = %scan(&method_modules., &_m., %str( ));

    %run_module(
      module     = &_mod.,
      troncal_id = &troncal_id.,
      split      = &split.,
      seg_id     = &seg_id.,
      run_id     = &run_id.
    );
  %end;

%mend run_method;
