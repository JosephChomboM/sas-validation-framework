/* =========================================================================
   run_module.sas — Dispatch: ejecuta un módulo en un contexto dado
   Parámetros:
     module     = nombre del módulo (gini, psi, etc.)
     troncal_id = id numérico de troncal
     split      = train | oot
     seg_id     = id numérico de segmento (vacío = universo/base)
     run_id     = identificador del run actual

   Convención: el módulo debe exponer %<module>_run(...).
   ========================================================================= */

%macro run_module(module=, troncal_id=, split=, seg_id=, run_id=);

  %put NOTE: -----------------------------------------------------;
  %put NOTE: [run_module] module=&module. troncal=&troncal_id. split=&split. seg_id=&seg_id.;
  %put NOTE: -----------------------------------------------------;

  /* Resolver input path */
  %fw_path_processed(outvar=_input_path, troncal_id=&troncal_id., split=&split., seg_id=&seg_id.);

  /* Construir scope label para naming de outputs */
  %if %superq(seg_id) = %then %let _scope = base;
  %else %let _scope = seg%sysfunc(putn(&seg_id., z3.));

  /* --- Incluir contract y run del módulo ----------------------------- */
  /* STUB: cuando los módulos existan, descomentar las líneas siguientes.
  %include "&fw_root./src/modules/&module./&module._contract.sas";
  %&module._run(
    input_path = &_input_path.,
    troncal_id = &troncal_id.,
    split      = &split.,
    scope      = &_scope.,
    run_id     = &run_id.
  );
  */

  %put NOTE: [run_module] (STUB) &module. completado para troncal_&troncal_id./&split./&_scope.;

%mend run_module;
