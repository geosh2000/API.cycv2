<?php
defined('BASEPATH') OR exit('No direct script access allowed');

class Meses extends CI_Controller {

        public function mes()
        {
                $meses = array(
                  'enero',
                  'febrero',
                  'marzo',
                  'abril',
                  'mayo',
                  'junio',
                  'julio',
                  'agosto',
                  'septiembre',
                  'octubre',
                  'noviembre',
                  'diciembre'
                );

                echo json_encode( $meses );

        }
}
