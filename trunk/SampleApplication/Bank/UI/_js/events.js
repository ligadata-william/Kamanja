


$(function() {

//    $('.slider-content').on('click', function() {
//        
//        prepareDrawChart($(this).attr('data-id'));
//
//        $("html, body").animate({ scrollTop: $(window).height() }, 600);
//    });

    $('.home').on('click', function() {

        $(window.location).attr('href', './landing.html');
    });

    $('.processing').on('click', function() {

        $(window.location).attr('href', './processing.html');
    });

});