/* =========================================================================
   select_methods.sas — Pseudo-step (contrato de UI)

   Este archivo NO es ejecutable ni generado por HTML.
   Documenta los IDs de UI (_id_*) que existirían en un .step equivalente
   para la selección de métodos/módulos a ejecutar.

   Ref: README.md §5, design.md §5
   ========================================================================= */

/* ---- IDs de UI documentados ------------------------------------------- */
/*                                                                          */
/*  _id_methods_select   : módulos seleccionados para el run               */
/*                          Tipo: lista texto (espacio-separado)            */
/*                          Valores posibles: gini psi (extensible)         */
/*                          Mapea a: macro var &methods_list.               */
/*                                                                          */
/*  _id_run_label        : etiqueta descriptiva opcional del run           */
/*                          Tipo: texto libre ($200)                        */
/*                          Mapea a: macro var &run_label.                  */
/*                                                                          */
/* ----------------------------------------------------------------------- */

/* (Opcional) Mapeo referencial a macro variables internas:
   %let methods_list = &_id_methods_select.;
   %let run_label    = &_id_run_label.;
   La selección real se define en config.sas o por el runner.
*/
