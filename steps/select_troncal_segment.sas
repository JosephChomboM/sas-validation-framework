/* =========================================================================
   select_troncal_segment.sas — Pseudo-step (contrato de UI)

   Este archivo NO es ejecutable ni generado por HTML.
   Documenta los IDs de UI (_id_*) que existirían en un .step equivalente
   para la selección de troncal y segmentación.  Se usa como estándar
   interno de naming para futuras herramientas o mapeo UI → SAS.

   Ref: README.md §5, design.md §5
   ========================================================================= */

/* ---- IDs de UI documentados ------------------------------------------- */
/*                                                                          */
/*  _id_troncal_select   : selector de troncal(es) a procesar              */
/*                          Tipo: lista numérica (ej. 1 2 3)               */
/*                          Mapea a: iteración sobre cfg_troncales          */
/*                                                                          */
/*  _id_troncal_list     : lista expandida de troncales seleccionadas      */
/*                          Tipo: texto (espacio-separado)                  */
/*                          Mapea a: macro var &troncal_list.               */
/*                                                                          */
/*  _id_segment_var      : variable segmentadora dentro de cada troncal    */
/*                          Tipo: nombre de columna ($64)                   */
/*                          Mapea a: cfg_troncales.var_seg                  */
/*                                                                          */
/*  _id_n_segments       : cantidad de segmentos numéricos (1..N)          */
/*                          Tipo: entero >=0                                */
/*                          Mapea a: cfg_troncales.n_segments               */
/*                                                                          */
/*  _id_train_min_mes    : mes mínimo ventana train (YYYYMM)              */
/*  _id_train_max_mes    : mes máximo ventana train                        */
/*  _id_oot_min_mes      : mes mínimo ventana oot                         */
/*  _id_oot_max_mes      : mes máximo ventana oot                         */
/*                          Mapean a: cfg_troncales.train_min_mes, etc.     */
/*                                                                          */
/* ----------------------------------------------------------------------- */

/* (Opcional) Mapeo referencial a macro variables internas:
   %let troncal_list = &_id_troncal_select.;
   La lógica real está en runner/main.sas y config.sas.
*/
