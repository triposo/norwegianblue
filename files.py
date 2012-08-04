def copy_with_replacement(src_path, dst_path, replacements):
  src_file = file(src_path)
  dst_file = file(dst_path, "w")
  content = as_unicode(src_file.read())
  content = replace_all(content, replacements)
  dst_file.write(content.encode('utf-8'))
  dst_file.close()
  src_file.close()

def copytree_with_replacement(src_dir, dst_dir, replacements=frozenset(), replace_extensions=None, ignores=frozenset({'.svn', '.git', 'gen', 'build'})):
  for src in os.listdir(src_dir):
    if src in ignores:
      continue
    src_path = os.path.join(src_dir, src)
    dst_path = os.path.join(dst_dir, src)
    if os.path.isdir(src_path):
      os.makedirs(dst_path)
      copytree_with_replacement(src_path, dst_path,
        replacements=replacements, replace_extensions=replace_extensions)
    else:
      dummy, ext = os.path.splitext(src_path)
      if ext in replace_extensions:
        copy_with_replacement(src_path, dst_path, replacements)
      else:
        shutil.copy(src_path, dst_path)


def replace_all(text, replacements):
  for (k, v) in replacements:
    text = text.replace(k, v)
  return text