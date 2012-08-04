import shutil
import os

def as_unicode(st):
  if type(st) != unicode:
    try:
      st.decode('utf-8', 'replace')
    except:
      print `st`
    return st.decode('utf-8', 'replace')
  return st

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

def download_if_changed(url, output_dir, local_name):
  """
  Download the url to the output_dir if it has changed according to the md5 ETag (this only really works for S3 URLs).
  """
  #output_dir = os.path.dirname(file_name)
  if not os.path.isdir(output_dir):
    os.makedirs(output_dir)
    # The [1:-1] is to strip leading and trailing " form the ETag value.
  remote_md5 = urllib2.urlopen(HeadRequest(url)).info()['etag'][1:-1]
  local_md5 = None
  local_path = os.path.join(output_dir, local_name)
  if os.path.isfile(local_path):
    with open(local_path) as f:
      local_md5 = md5.md5(f.read()).hexdigest()
  if remote_md5 != local_md5:
    print '%s -> %s' % (url, local_name)
    if not os.path.isdir(output_dir):
      os.makedirs(output_dir)
    subprocess.check_call('cd %s && curl -o "%s" "%s"' % (output_dir, local_name, url), shell=True)
    with open(local_path) as f:
      local_md5 = md5.md5(f.read()).hexdigest()
    if local_md5 != remote_md5:
      raise StandardError('Download failed, md5 checksum does not match: %s' % file)
  return local_path
