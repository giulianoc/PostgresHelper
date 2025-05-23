
# Copyright (C) Giuliano Catrambone (giulianocatrambone@gmail.com)

# This program is free software; you can redistribute it and/or 
# modify it under the terms of the GNU General Public License 
# as published by the Free Software Foundation; either 
# version 2 of the License, or (at your option) any later 
# version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.

# Commercial use other than under the terms of the GNU General Public
# License is allowed only after express negotiation of conditions
# with the authors.

SET (SOURCES
	PostgresHelper.cpp
)

SET (HEADERS
	PostgresHelper.h
)

include_directories("${SPDLOG_INCLUDE_DIR}")
include_directories("${NLOHMANN_INCLUDE_DIR}")
include_directories("${LIBPQXX_INCLUDE_DIR}")
include_directories("${STRINGUTILS_INCLUDE_DIR}")
include_directories("${JSONUTILS_INCLUDE_DIR}")
include_directories("${DBCONN_INCLUDE_DIR}")

if(APPLE)
  link_directories("${STRINGUTILS_LIB_DIR}")
  link_directories("${JSONUTILS_LIB_DIR}")
  link_directories("/opt/homebrew/opt/libpq/lib")
  link_directories("${PROJECT_SOURCE_DIR}/../deploy/libpqxx/lib")
endif()

add_library (PostgresHelper SHARED ${SOURCES} ${HEADERS})

if(APPLE)
  target_link_libraries(PostgresHelper StringUtils)
  target_link_libraries(PostgresHelper JSONUtils)
  target_link_libraries(PostgresHelper pqxx)
  target_link_libraries(PostgresHelper pq)
endif()

if(ZORAC)
	install (TARGETS PostgresHelper DESTINATION services/cms-import)
	install (TARGETS PostgresHelper DESTINATION services/cms-pusher)
	install (TARGETS PostgresHelper DESTINATION services/cms-getter)
else()
  install (TARGETS PostgresHelper DESTINATION lib)
  install (FILES ${HEADERS} DESTINATION include)
endif()

