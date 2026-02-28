/* =========================================================================
   run_module.sas — Dispatch: ejecuta un módulo en un contexto dado

   Ciclo de vida por invocación (create → promote → work → drop):
     1) Resolver ruta relativa del input en CASLIB PROC
     2) Promote: cargar .sashdat → tabla CAS promovida (_active_input)
     3) Ejecutar módulo (lee _active_input de PROC, escribe a OUT)
     4) Drop: eliminar tabla promovida de PROC

   Parámetros:
     module     = nombre del módulo (gini, psi, etc.)
     troncal_id = id numérico de troncal
     split      = train | oot
     seg_id     = id numérico de segmento (vacío = universo/base)
     run_id     = identificador del run actual

   CASLIB policy:
     - Input se lee desde CASLIB PROC (PATH → data/processed/, subdirs=1).
     - Output se escribe en CASLIB OUT o en un CASLIB scoped del
       módulo (ej. MOD_GINI_<run_id>), siguiendo caslib_lifecycle.md.
     - casuser NO se usa para datos operativos.
     - CASLIBs PROC y OUT deben estar creados antes de llamar esta macro.

   Convención: el módulo debe exponer %<module>_run(...).
   ========================================================================= */

%macro run_module(module=, troncal_id=, split=, seg_id=, run_id=);

  %put NOTE: -----------------------------------------------------;
  %put NOTE: [run_module] module=&module. troncal=&troncal_id. split=&split. seg_id=&seg_id.;
  %put NOTE: -----------------------------------------------------;

  /* 1) Resolver input path (relativo al CASLIB PROC) */
  %fw_path_processed(outvar=_input_path, troncal_id=&troncal_id., split=&split., seg_id=&seg_id.);

  /* Construir scope label para naming de outputs */
  %if %superq(seg_id) = %then %let _scope = base;
  %else %let _scope = seg%sysfunc(putn(&seg_id., z3.));

  /* Nombre del CASLIB de output del run */
  %let _out_caslib = OUT;

  /* 2) Promote: cargar .sashdat desde PROC y promover como _active_input */
  %_promote_castable(
    m_cas_sess_name = conn,
    m_input_caslib  = PROC,
    m_subdir_data   = %sysfunc(tranwrd(&_input_path., .sashdat, )),
    m_output_caslib = PROC,
    m_output_data   = _active_input
  );

  /* 3) Ejecutar módulo ---------------------------------------------------
     El módulo lee de PROC._active_input y escribe en &_out_caslib.
     Si necesita un CASLIB scoped propio, lo crea y limpia internamente.

     STUB: cuando los módulos existan, descomentar las líneas siguientes.

  %include "&fw_root./src/modules/&module./&module._contract.sas";
  %&module._run(
    input_caslib = PROC,
    input_table  = _active_input,
    output_caslib= &_out_caslib.,
    troncal_id   = &troncal_id.,
    split        = &split.,
    scope        = &_scope.,
    run_id       = &run_id.
  );
  */

  %put NOTE: [run_module] (STUB) &module. completado para troncal_&troncal_id./&split./&_scope.;

  /* 4) Drop: eliminar tabla promovida de PROC */
  proc cas;
    table.dropTable / caslib="PROC" name="_active_input" quiet=true;
  quit;

%mend run_module;
