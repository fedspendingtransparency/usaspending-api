/*jslint browser: true, nomen: true, white: true */
/*globals jQuery */

(function ($) {
  "use strict";
  function _findInput(el) {
    return $(el).find('input, textarea');
  }

  function _findLabel(el) {
    return $(el).find('label');
  }

  function _findSelect(el) {
    return $(el).find('select');
  }


  var FlyLabel = (function () {
    function _FlyLabel(el) {
      // Set things
      this.el = el;
      this.input = _findInput(this.el);
      if (this.input.length !== 1) {
        this.input = _findSelect(this.el);
      }
      this.label = _findLabel(this.el);
      this._onKeyUp();
      // Set up handlers.
      this._bindEvents();
    }

    _FlyLabel.prototype = {
      _bindEvents: function () {
        this.input.on('keyup change', $.proxy(this._onKeyUp, this));
        this.input.on('blur', $.proxy(this._onBlur, this));
        this.input.on('focus', $.proxy(this._onFocus, this));
      },
      _onKeyUp: function (ev) {
        if (this.input.val() === '') {
          this.label.removeClass('is-active');
        } else {
          this.label.addClass('is-active');
        }
        ev && ev.preventDefault();
      },
      _onFocus: function (ev) {
        this.label.addClass('has-focus');
        this._onKeyUp();
        ev && ev.preventDefault();
      },
      _onBlur: function (ev) {
        this.label.removeClass('has-focus');
        this._onKeyUp();
        ev && ev.preventDefault();
      }
    };
    return _FlyLabel;
  }());

  $.fn.flyLabels = function () {
    this.find('.fly-group').each(function () {
      return new FlyLabel(this);
    });
  };

}(window.jQuery || window.$));
