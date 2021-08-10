#include "utils/hist_helper.h"

thread_local std::vector<hist_helper*> hist_helper::_helpers = {};
thread_local ss::timer<> hist_helper::_printing_timer = ss::timer<>{};
thread_local ss::timer<> hist_helper::_reset_timer = ss::timer<>{};
