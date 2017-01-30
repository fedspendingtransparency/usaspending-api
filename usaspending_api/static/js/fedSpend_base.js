// BOOTSTRAP TOOLTIP AND POPOVER
$(function() {
    $('[data-toggle="tooltip"]').tooltip()
})

$(function() {
    $('[data-toggle="popover"]').popover()

    $('body').on('click', function(e) {
        $('[data-toggle="popover"]').each(function() {
            //the 'is' for buttons that trigger popups
            //the 'has' for icons within a button that triggers a popup
            if (!$(this).is(e.target) && $(this).has(e.target).length === 0 && $('.popover').has(e.target).length === 0) {
                $(this).popover('hide');
            }
        });
    });
});


// SET HEIGHT OF BLOCKS ON HOMEPAGE
var shortCol = $('.short-col').outerHeight()
var tallCol = $('.tall-col').outerHeight()

function numbers(colA, colB) {
    if (colA > colB) {
        $(".tall-col").css({
            'height': ($(".short-col").outerHeight() + 'px')
        });
    } else if (colA === colB) {
        // console.log('The divs are the same height. Wow, what are the chances?');
    } else {
        $(".short-col").css({
            'height': ($(".tall-col").outerHeight() + 'px')
        });
    }
}

$(document).ready(function() {

// $('.code-block-toggle-html, .code-block-toggle-sass').collapse({
//   toggle: true
// })

// function checkBtn() {
//     if ($(".code-block-toggle-html:not(.collapsed)")) {
//         $(".code-block-toggle-html").removeClass("active");
//         console.log("foo");
//     } else {
//         $(".code-block-toggle-html").removeClass("active");
//     }
// }

// checkBtn();

    checkSize();
    $(window).resize(checkSize);

});

//Function to the css rule
function checkSize() {
    if ($(".menu-toggle").css("display") == "block") {
        numbers(shortCol, tallCol);
    }
    if ($(".col-md-4, .col-md-5").css("float") == "left") {
        numbers(shortCol, tallCol);
    }
}



if ($('#back-to-top').length) {
    var scrollTrigger = 1250, // px
        backToTop = function() {
            var scrollTop = $(window).scrollTop();
            if (scrollTop > scrollTrigger) {
                $('#back-to-top').addClass('show');
            } else {
                $('#back-to-top').removeClass('show');
            }
        };
    backToTop();
    $(window).on('scroll', function() {
        backToTop();
    });
    $('#back-to-top').on('click', function(e) {
        e.preventDefault();
        $('html,body').animate({
            scrollTop: 1250
        }, 700);
    });
}


function placeFooter() {
    var winHeight = window.innerHeight;
    var divHeight = $(".usa-da-outter-wrap").height();
    var footerHeight = $(".usa-da-footer").innerHeight() + 20;

    if (winHeight >= divHeight) {
        $(".usa-da-footer").addClass("place-bottom").css("bottom", -footerHeight);
    } else if (winHeight <= divHeight) {
        $(".usa-da-footer").removeClass("place-bottom");
    } else if (divHeight >= winHeight) {
        $(".usa-da-footer").removeClass("place-bottom");
    }

}

$(window).load(function() {
    placeFooter()
});

$(window).resize(function() {
    placeFooter();
});



/* activate sidebar */
$('#sidebar').affix({
    offset: {
        top: 26
    }
});

/* activate scrollspy menu */
var $body = $(document.body);
var navHeight = $('.navbar').outerHeight(true) + 70;


$body.scrollspy({
    target: '#leftCol',
    offset: navHeight
});



/* smooth scrolling sections */
$('ul.nav li a[href*="#"]:not([href="#"])').click(function() {
    if (location.pathname.replace(/^\//, '') == this.pathname.replace(/^\//, '') && location.hostname == this.hostname) {
        var target = $(this.hash);
        target = target.length ? target : $('[name=' + this.hash.slice(1) + ']');
        if (target.length) {
            $('html,body').animate({
                scrollTop: target.offset().top - 126
            }, 1000);
            return false;
        }
    }
});



/* add active class to the first item in the side nav if the window height is less than 53px */
$(window).scroll(function() {
    if ($(window).scrollTop() <= 53) {
        $('ul#sidebar li:first-child').addClass('active');
    }
    if ($(window).scrollTop() >= 10) {
        $('.navbar').addClass('nav-shadow');
    } else {
        $('.navbar').removeClass('nav-shadow');
    }
});



/* copy sass variable for color to clipboard */
 function clip(elementId) {
     var aux = document.createElement("input");
     aux.setAttribute("value", document.getElementById(elementId).innerHTML);
     document.body.appendChild(aux);
     aux.select();
     document.execCommand("copy");
     document.body.removeChild(aux);
     // console.log(aux);
     // var bla = aux;
     // console.log("new" + bla);
 };

$(document).ready(function() {
    $('.color-block').on( "click", function() {
        // console.log("it worked");
        var clipVar = $(this).find('li:first-child').attr('id');
        clip(clipVar)
        $(".alertMsg").fadeIn(1000).delay(2000).fadeOut(1000);
        // document.getElementById("p1").innerHTML = clipHex;
    });
 });

