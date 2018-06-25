<?php

if( !defined( 'BASEPATH' ) ) exit('No direct script access allowed');

$config = array(

      'ausentismo_put' => array(
            array( 'field' => 'inicio',     'label' => 'inicio',    'rules' => 'trim|required' ),
            array( 'field' => 'fin',        'label' => 'fin',       'rules' => 'trim|required' ),
            array( 'field' => 'tipo',       'label' => 'tipo',      'rules' => 'trim|required|numeric|is_natural' ),
            array( 'field' => 'dias',       'label' => 'dias',      'rules' => 'trim|required|numeric|is_natural' ),
            array( 'field' => 'descansos',  'label' => 'descansos', 'rules' => 'trim|required|numeric|is_natural' ),
            array( 'field' => 'caso',       'label' => 'caso',      'rules' => 'trim' ),
            array( 'field' => 'notas',      'label' => 'notas',     'rules' => 'trim' ),
            array( 'field' => 'motivo',     'label' => 'motivo',    'rules' => 'trim' ),
            array( 'field' => 'asesor',     'label' => 'asesor',    'rules' => 'trim|required|numeric|is_natural' )
      )

);
