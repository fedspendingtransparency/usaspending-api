# from usaspending_api.common.renderers import BASE_DIR, BASE_URL, DocumentAPIRenderer
#
#
# def test_document_api_renderer(temp_file_path):
#     r = DocumentAPIRenderer()
#
#     # Don't know how to test this since it's going to search a bunch of paths for a file.
#     # def get_context(self, *args, **kwargs):
#
#     assert r._get_description_and_endpoint_doc({"description": ""}) == ("", "")
#     assert r._get_description_and_endpoint_doc({"description": "test"}) == ("test", "")
#     assert r._get_description_and_endpoint_doc({"description": "<p>test</p>"}) == ("<p>test</p>", "")
#     assert r._get_description_and_endpoint_doc({"description": "<p>test</p><p></p>"}) == ("<p>test</p>", "")
#     assert r._get_description_and_endpoint_doc(
#         {"description": "<p>test</p><p></p><p>endpoint_doc: file</p>"}
#     ) == ("<p>test</p>", "file")
#
#     assert r._get_inferred_doc("/api/v9/test") == "test.md"
#
#     assert r._get_api_version("/api/v9/test") == "v9"
#
#     # Not sure how to test this.  It's searching paths looking for a file.
#     # assert r._get_doc_path(version, docs)
#
#     # The most we can do here is test that something was returned.
#     assert r._get_current_git_branch() not in ("", None)
#
#     assert r._get_github_url("test", "dev") == ""
#     assert r._get_github_url(BASE_DIR, "dev") == BASE_URL.format(git_branch="dev")
#
#     assert r._update_description("test", "") == "test"
#     assert r._update_description("", "") == ""
#     with open(temp_file_path, "w") as f:
#         f.writelines(["FORMAT\n", "HOST\n", "#\n", "+\n", "*\n", "-\n"])
#     assert r._update_description("", temp_file_path) == ""
#     with open(temp_file_path, "w") as f:
#         f.writelines(["FORMAT\n", "HOST\n", "#\n", "+\n", "*\n", "test\n", "-\n"])
#     assert r._update_description("", temp_file_path) == "<p>test</p>"
#     with open(temp_file_path, "w") as f:
#         f.writelines(["FORMAT", "HOST\n", "#\n", "+\n", "*\n", "test\n", "test\n", "-\n"])
#     assert r._update_description("", temp_file_path) == "<p>test test</p>"
#
#     _url = BASE_URL + "/here"
#     assert r._add_url_to_description(
#         "test", "url"
#     ) == 'test\n<p>Documentation for this endpoint can be found <a href="url">here</a>.</p>'.format(_url)
#     assert r._add_url_to_description("test", "") == 'test'
