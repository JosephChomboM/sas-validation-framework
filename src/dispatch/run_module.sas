/* =========================================================================
   run_module.sas — Dispatch: ejecuta un módulo en un contexto dado
   Parámetros:
     module     = nombre del módulo (gini, psi, etc.)
     troncal_id = id numérico de troncal
     split      = train | oot
     seg_id     = id numérico de segmento (vacío = universo/base)
     run_id     = identificador del run actual

   CASLIB policy:
     - Input se lee desde CASLIB PROCESSED (PATH → data/processed/, subdirs=1).
     - Output se escribe en CASLIB OUT_<run_id> o en un CASLIB scoped del
       módulo (ej. MOD_GINI_<run_id>), siguiendo caslib_lifecycle.md.
     - casuser NO se usa para datos operativos.

   Convención: el módulo debe exponer %<module>_run(...).
   ========================================================================= */

%macro run_module(module=, troncal_id=, split=, seg_id=, run_id=);

  %put NOTE: -----------------------------------------------------;
  %put NOTE: [run_module] module=&module. troncal=&troncal_id. split=&split. seg_id=&seg_id.;
  %put NOTE: -----------------------------------------------------;

  /* Resolver input path (relativo al CASLIB PROCESSED) */
  %fw_path_processed(outvar=_input_path, troncal_id=&troncal_id., split=&split., seg_id=&seg_id.);

  /* Construir scope label para naming de outputs */
  %if %superq(seg_id) = %then %let _scope = base;
  %else %let _scope = seg%sysfunc(putn(&seg_id., z3.));

  /* Nombre del CASLIB de output del run */
  %let _out_caslib = OUT_&run_id.;

  /* --- Incluir contract y run del módulo ----------------------------- */
  /* STUB: cuando los módulos existan, descomentar las líneas siguientes.
     El módulo leerá de CASLIB PROCESSED y escribirá en &_out_caslib.
     Si necesita un CASLIB scoped propio, lo crea y limpia internamente.

  %include "&fw_root./src/modules/&module./&module._contract.sas";
  %&module._run(
    input_caslib = PROCESSED,
    input_path   = &_input_path.,
    output_caslib= &_out_caslib.,
    troncal_id   = &troncal_id.,
    split        = &split.,
    scope        = &_scope.,
    run_id       = &run_id.
  );
  */

  %put NOTE: [run_module] (STUB) &module. completado para troncal_&troncal_id./&split./&_scope.;

%mend run_module;
