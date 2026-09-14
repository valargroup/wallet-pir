
/opt/transparent-publisher-build/shoup-release-20260911/artifacts/transparent-shard-server:     file format elf64-x86-64


Disassembly of section .text:

000000000039cfd0 <valar_spiral_rs::ntt::avx2::ntt_forward>:
  39cfd0:	push   %rbp
  39cfd1:	push   %r15
  39cfd3:	push   %r14
  39cfd5:	push   %r13
  39cfd7:	push   %r12
  39cfd9:	push   %rbx
  39cfda:	sub    $0xd8,%rsp
  39cfe1:	mov    %rsi,0x8(%rsp)
  39cfe6:	mov    0x40(%rdi),%r10
  39cfea:	cmp    $0x1,%r10
  39cfee:	jne    39d2e3 <valar_spiral_rs::ntt::avx2::ntt_forward+0x313>
  39cff4:	mov    0x38(%rdi),%r8
  39cff8:	mov    %r8d,%r10d
  39cffb:	and    $0x3f,%r10d
  39cfff:	mov    $0x1,%eax
  39d004:	shlx   %r10,%rax,%r9
  39d009:	mov    0x8(%rdi),%rcx
  39d00d:	mov    0x10(%rdi),%rsi
  39d011:	xor    %eax,%eax
  39d013:	mov    %r8,0x30(%rsp)
  39d018:	test   %r8,%r8
  39d01b:	setne  %r8b
  39d01f:	je     39d9cc <valar_spiral_rs::ntt::avx2::ntt_forward+0x9fc>
  39d025:	cmp    %rdx,%r9
  39d028:	ja     39dc12 <valar_spiral_rs::ntt::avx2::ntt_forward+0xc42>
  39d02e:	test   %rsi,%rsi
  39d031:	je     39dcdb <valar_spiral_rs::ntt::avx2::ntt_forward+0xd0b>
  39d037:	mov    0x10(%rcx),%rdx
  39d03b:	test   %rdx,%rdx
  39d03e:	je     39dc81 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcb1>
  39d044:	mov    %r10,0x68(%rsp)
  39d049:	cmp    $0x1,%rdx
  39d04d:	je     39dc94 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcc4>
  39d053:	mov    %r8b,%al
  39d056:	mov    0x8(%rcx),%rcx
  39d05a:	mov    0x8(%rcx),%rdx
  39d05e:	mov    %rdx,0x48(%rsp)
  39d063:	mov    0x10(%rcx),%rdx
  39d067:	mov    %rdx,0x20(%rsp)
  39d06c:	mov    0x20(%rcx),%rdx
  39d070:	mov    %rdx,0x40(%rsp)
  39d075:	mov    0x28(%rcx),%rcx
  39d079:	mov    %rcx,0x18(%rsp)
  39d07e:	mov    0xa8(%rdi),%rcx
  39d085:	mov    %rcx,0x28(%rsp)
  39d08a:	add    %rcx,%rcx
  39d08d:	mov    %rcx,(%rsp)
  39d091:	xor    %ecx,%ecx
  39d093:	mov    $0x1,%edx
  39d098:	mov    %r9,0x70(%rsp)
  39d09d:	jmp    39d0bf <valar_spiral_rs::ntt::avx2::ntt_forward+0xef>
  39d09f:	nop
  39d0a0:	mov    0x38(%rsp),%rsi
  39d0a5:	lea    0x1(%rsi),%rax
  39d0a9:	mov    %rax,%rdx
  39d0ac:	mov    %rsi,%rcx
  39d0af:	cmp    0x30(%rsp),%rsi
  39d0b4:	mov    0x70(%rsp),%r9
  39d0b9:	je     39d970 <valar_spiral_rs::ntt::avx2::ntt_forward+0x9a0>
  39d0bf:	shrx   %rdx,%r9,%rbx
  39d0c4:	mov    %rbx,%rsi
  39d0c7:	add    %rbx,%rsi
  39d0ca:	mov    0x8(%rsp),%r15
  39d0cf:	je     39dbdb <valar_spiral_rs::ntt::avx2::ntt_forward+0xc0b>
  39d0d5:	mov    %rax,0x38(%rsp)
  39d0da:	mov    $0x1,%eax
  39d0df:	shlx   %rcx,%rax,%rdx
  39d0e4:	mov    %rsi,%r8
  39d0e7:	neg    %r8
  39d0ea:	and    %r9,%r8
  39d0ed:	test   %rbx,%rbx
  39d0f0:	je     39d2a0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x2d0>
  39d0f6:	mov    %rbx,%rax
  39d0f9:	shl    $0x4,%rax
  39d0fd:	mov    %rax,0x50(%rsp)
  39d102:	lea    0x0(,%rbx,8),%rax
  39d10a:	mov    %rax,0x58(%rsp)
  39d10f:	mov    $0x1,%eax
  39d114:	mov    %r15,0x10(%rsp)
  39d119:	xor    %edi,%edi
  39d11b:	mov    %rdx,0x78(%rsp)
  39d120:	mov    %rsi,0x90(%rsp)
  39d128:	nopl   0x0(%rax,%rax,1)
  39d130:	add    %rdx,%rdi
  39d133:	cmp    0x20(%rsp),%rdi
  39d138:	jae    39dc3e <valar_spiral_rs::ntt::avx2::ntt_forward+0xc6e>
  39d13e:	cmp    0x18(%rsp),%rdi
  39d143:	jae    39dc5e <valar_spiral_rs::ntt::avx2::ntt_forward+0xc8e>
  39d149:	sub    %rsi,%r8
  39d14c:	jb     39dc23 <valar_spiral_rs::ntt::avx2::ntt_forward+0xc53>
  39d152:	mov    %rax,0x80(%rsp)
  39d15a:	mov    %r8,0x88(%rsp)
  39d162:	mov    0x48(%rsp),%rax
  39d167:	mov    (%rax,%rdi,8),%rax
  39d16b:	mov    %rax,0xa0(%rsp)
  39d173:	mov    0x40(%rsp),%rax
  39d178:	mov    (%rax,%rdi,8),%rax
  39d17c:	mov    %rax,0x98(%rsp)
  39d184:	mov    0x58(%rsp),%rax
  39d189:	mov    0x10(%rsp),%rcx
  39d18e:	add    %rcx,%rax
  39d191:	mov    %rax,0x60(%rsp)
  39d196:	xor    %ebp,%ebp
  39d198:	nopl   0x0(%rax,%rax,1)
  39d1a0:	cmp    %rbp,%rsi
  39d1a3:	je     39dbcc <valar_spiral_rs::ntt::avx2::ntt_forward+0xbfc>
  39d1a9:	lea    (%rbx,%rbp,1),%rdi
  39d1ad:	cmp    %rsi,%rdi
  39d1b0:	jae    39dbc0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xbf0>
  39d1b6:	mov    0x10(%rsp),%rax
  39d1bb:	mov    (%rax,%rbp,8),%r12
  39d1bf:	mov    0x60(%rsp),%rax
  39d1c4:	mov    (%rax,%rbp,8),%rdx
  39d1c8:	mov    (%rsp),%r15
  39d1cc:	cmp    %r15,%r12
  39d1cf:	mov    $0x0,%ecx
  39d1d4:	cmovae %r15,%rcx
  39d1d8:	mov    0x98(%rsp),%rax
  39d1e0:	mulx   %rax,%rax,%rax
  39d1e5:	mulx   0xa0(%rsp),%r14,%rsi
  39d1ef:	sub    %rcx,%r12
  39d1f2:	mov    %rax,%rdx
  39d1f5:	mov    0x28(%rsp),%rdi
  39d1fa:	mulx   %rdi,%rcx,%rax
  39d1ff:	sub    %rcx,%r14
  39d202:	sbb    %rax,%rsi
  39d205:	mov    %r14,%r13
  39d208:	sub    %rdi,%r13
  39d20b:	sbb    $0x0,%rsi
  39d20f:	setb   %al
  39d212:	movzbl %al,%edi
  39d215:	call   39cfc0 <subtle::black_box>
  39d21a:	mov    0x90(%rsp),%rsi
  39d222:	movzbl %al,%eax
  39d225:	mov    %rax,%rcx
  39d228:	neg    %rcx
  39d22b:	dec    %rax
  39d22e:	and    %r13,%rax
  39d231:	and    %r14,%rcx
  39d234:	or     %rax,%rcx
  39d237:	lea    (%r12,%rcx,1),%rax
  39d23b:	mov    0x10(%rsp),%rdx
  39d240:	mov    %rax,(%rdx,%rbp,8)
  39d244:	add    %r15,%r12
  39d247:	sub    %rcx,%r12
  39d24a:	mov    0x60(%rsp),%rax
  39d24f:	mov    %r12,(%rax,%rbp,8)
  39d253:	inc    %rbp
  39d256:	cmp    %rbp,%rbx
  39d259:	jne    39d1a0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x1d0>
  39d25f:	mov    0x78(%rsp),%rdx
  39d264:	mov    0x80(%rsp),%rdi
  39d26c:	cmp    %rdx,%rdi
  39d26f:	mov    %rdi,%rax
  39d272:	adc    $0x0,%rax
  39d276:	mov    0x10(%rsp),%rcx
  39d27b:	add    0x50(%rsp),%rcx
  39d280:	mov    %rcx,0x10(%rsp)
  39d285:	cmp    %rdx,%rdi
  39d288:	mov    0x88(%rsp),%r8
  39d290:	jb     39d130 <valar_spiral_rs::ntt::avx2::ntt_forward+0x160>
  39d296:	jmp    39d0a0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xd0>
  39d29b:	nopl   0x0(%rax,%rax,1)
  39d2a0:	add    %rsi,%r8
  39d2a3:	xor    %eax,%eax
  39d2a5:	data16 cs nopw 0x0(%rax,%rax,1)
  39d2b0:	lea    (%rdx,%rax,1),%rcx
  39d2b4:	cmp    0x20(%rsp),%rcx
  39d2b9:	jae    39dc2f <valar_spiral_rs::ntt::avx2::ntt_forward+0xc5f>
  39d2bf:	cmp    0x18(%rsp),%rcx
  39d2c4:	jae    39dc4f <valar_spiral_rs::ntt::avx2::ntt_forward+0xc7f>
  39d2ca:	sub    %rsi,%r8
  39d2cd:	cmp    %rsi,%r8
  39d2d0:	jb     39dc23 <valar_spiral_rs::ntt::avx2::ntt_forward+0xc53>
  39d2d6:	inc    %rax
  39d2d9:	cmp    %rdx,%rax
  39d2dc:	jb     39d2b0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x2e0>
  39d2de:	jmp    39d0a0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xd0>
  39d2e3:	test   %r10,%r10
  39d2e6:	mov    0x8(%rsp),%r8
  39d2eb:	je     39dbab <valar_spiral_rs::ntt::avx2::ntt_forward+0xbdb>
  39d2f1:	mov    0x38(%rdi),%r11
  39d2f5:	mov    %r11d,%ebx
  39d2f8:	and    $0x3f,%ebx
  39d2fb:	mov    $0x1,%eax
  39d300:	shlx   %rbx,%rax,%rsi
  39d305:	mov    0x8(%rdi),%r15
  39d309:	mov    0x10(%rdi),%rax
  39d30d:	mov    %rax,0x58(%rsp)
  39d312:	mov    %rsi,%rax
  39d315:	shr    $0x2,%rax
  39d319:	cmp    $0x2,%ebx
  39d31c:	adc    $0x0,%rax
  39d320:	mov    %rax,0x30(%rsp)
  39d325:	mov    %rsi,%r12
  39d328:	and    $0xfffffffffffffff0,%r12
  39d32c:	mov    %esi,%eax
  39d32e:	and    $0xc,%eax
  39d331:	mov    %rax,0x20(%rsp)
  39d336:	lea    0x60(%r8),%rax
  39d33a:	mov    %rax,0x68(%rsp)
  39d33f:	xor    %ebp,%ebp
  39d341:	vbroadcasti128 -0x35b27a(%rip),%ymm0        # 420d0 <GCC_except_table4170+0xfec>
  39d34a:	vpbroadcastq -0x35a133(%rip),%ymm1        # 43220 <GCC_except_table4170+0x213c>
  39d353:	mov    $0x1,%eax
  39d358:	xor    %ecx,%ecx
  39d35a:	mov    %rdi,0x50(%rsp)
  39d35f:	mov    %r10,0x48(%rsp)
  39d364:	mov    %r11,0x90(%rsp)
  39d36c:	mov    %rbx,0x40(%rsp)
  39d371:	mov    %rsi,0x88(%rsp)
  39d379:	mov    %r15,0x38(%rsp)
  39d37e:	mov    %r12,0x70(%rsp)
  39d383:	jmp    39d3a8 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3d8>
  39d385:	data16 cs nopw 0x0(%rax,%rax,1)
  39d390:	mov    0x18(%rsp),%rcx
  39d395:	cmp    %r10,%rcx
  39d398:	mov    %rcx,%rax
  39d39b:	adc    $0x0,%rax
  39d39f:	cmp    %r10,%rcx
  39d3a2:	jae    39dbab <valar_spiral_rs::ntt::avx2::ntt_forward+0xbdb>
  39d3a8:	cmp    0x58(%rsp),%rcx
  39d3ad:	jae    39dcad <valar_spiral_rs::ntt::avx2::ntt_forward+0xcdd>
  39d3b3:	mov    %rax,%r9
  39d3b6:	lea    (%rcx,%rcx,2),%rax
  39d3ba:	mov    0x10(%r15,%rax,8),%rdx
  39d3bf:	test   %rdx,%rdx
  39d3c2:	je     39dc81 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcb1>
  39d3c8:	cmp    $0x1,%rdx
  39d3cc:	je     39dc94 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcc4>
  39d3d2:	mov    %r9,0x18(%rsp)
  39d3d7:	cmp    $0x3,%rcx
  39d3db:	ja     39dcc4 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcf4>
  39d3e1:	shlx   %rbx,%rcx,%r14
  39d3e6:	lea    (%r8,%r14,8),%rdx
  39d3ea:	mov    %rdx,(%rsp)
  39d3ee:	mov    0xa8(%rdi,%rcx,8),%rdx
  39d3f6:	lea    (%rdx,%rdx,1),%ecx
  39d3f9:	mov    %edx,%r9d
  39d3fc:	test   %r11,%r11
  39d3ff:	je     39d782 <valar_spiral_rs::ntt::avx2::ntt_forward+0x7b2>
  39d405:	lea    (%r15,%rax,8),%rax
  39d409:	mov    0x8(%rax),%rax
  39d40d:	mov    0x8(%rax),%r15
  39d411:	mov    0x20(%rax),%r13
  39d415:	vmovq  %rcx,%xmm2
  39d41a:	vpbroadcastq %xmm2,%ymm2
  39d41f:	vmovq  %r9,%xmm3
  39d424:	vpbroadcastq %xmm3,%ymm3
  39d429:	vmovd  %ecx,%xmm4
  39d42d:	vpbroadcastd %xmm4,%xmm4
  39d432:	lea    (%r8,%r14,8),%rax
  39d436:	mov    %rax,0x80(%rsp)
  39d43e:	shl    $0x3,%r14
  39d442:	mov    %r14,0x78(%rsp)
  39d447:	mov    $0x1,%eax
  39d44c:	xor    %edx,%edx
  39d44e:	mov    %r15,0x60(%rsp)
  39d453:	mov    %r13,0xa0(%rsp)
  39d45b:	jmp    39d482 <valar_spiral_rs::ntt::avx2::ntt_forward+0x4b2>
  39d45d:	nopl   (%rax)
  39d460:	mov    0x28(%rsp),%rdx
  39d465:	lea    0x1(%rdx),%rax
  39d469:	mov    0x90(%rsp),%r11
  39d471:	cmp    %r11,%rdx
  39d474:	mov    0x88(%rsp),%rsi
  39d47c:	je     39d750 <valar_spiral_rs::ntt::avx2::ntt_forward+0x780>
  39d482:	mov    $0x1,%edi
  39d487:	shlx   %rdx,%rdi,%rbx
  39d48c:	mov    %rax,%rdx
  39d48f:	cmp    $0xb,%r11
  39d493:	setb   %al
  39d496:	mov    %rdx,0x28(%rsp)
  39d49b:	shrx   %rdx,%rsi,%r12
  39d4a0:	cmp    $0x4,%r12
  39d4a4:	setb   %dl
  39d4a7:	or     %al,%dl
  39d4a9:	mov    %r12,%rax
  39d4ac:	shr    $0x2,%rax
  39d4b0:	mov    %r12d,%esi
  39d4b3:	and    $0x3,%esi
  39d4b6:	cmp    $0x1,%rsi
  39d4ba:	sbb    $0xffffffffffffffff,%rax
  39d4be:	test   %dl,%dl
  39d4c0:	je     39d670 <valar_spiral_rs::ntt::avx2::ntt_forward+0x6a0>
  39d4c6:	test   %r12,%r12
  39d4c9:	je     39d460 <valar_spiral_rs::ntt::avx2::ntt_forward+0x490>
  39d4cb:	mov    %r12,%r8
  39d4ce:	and    $0xfffffffffffffffc,%r8
  39d4d2:	mov    %r12,%rax
  39d4d5:	shl    $0x4,%rax
  39d4d9:	mov    %rax,0x10(%rsp)
  39d4de:	lea    0x0(,%r12,8),%rax
  39d4e6:	mov    %rax,0x98(%rsp)
  39d4ee:	mov    0x80(%rsp),%rax
  39d4f6:	lea    (%rax,%r12,8),%rax
  39d4fa:	mov    (%rsp),%r14
  39d4fe:	xor    %edx,%edx
  39d500:	jmp    39d53b <valar_spiral_rs::ntt::avx2::ntt_forward+0x56b>
  39d502:	data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  39d510:	cmp    %rbx,%rdx
  39d513:	mov    %rdx,%rdi
  39d516:	adc    $0x0,%rdi
  39d51a:	mov    0x10(%rsp),%rsi
  39d51f:	add    %rsi,%r14
  39d522:	add    %rsi,%rax
  39d525:	cmp    %rbx,%rdx
  39d528:	mov    0x60(%rsp),%r15
  39d52d:	mov    0xa0(%rsp),%r13
  39d535:	jae    39d460 <valar_spiral_rs::ntt::avx2::ntt_forward+0x490>
  39d53b:	add    %rbx,%rdx
  39d53e:	mov    (%r15,%rdx,8),%rsi
  39d542:	mov    0x0(%r13,%rdx,8),%r15
  39d547:	mov    %rdi,%rdx
  39d54a:	cmp    $0x4,%r12
  39d54e:	jae    39d560 <valar_spiral_rs::ntt::avx2::ntt_forward+0x590>
  39d550:	xor    %r13d,%r13d
  39d553:	jmp    39d620 <valar_spiral_rs::ntt::avx2::ntt_forward+0x650>
  39d558:	nopl   0x0(%rax,%rax,1)
  39d560:	vmovq  %rsi,%xmm5
  39d565:	vpbroadcastq %xmm5,%ymm5
  39d56a:	vmovq  %r15,%xmm6
  39d56f:	vpbroadcastq %xmm6,%ymm6
  39d574:	mov    0x98(%rsp),%rdi
  39d57c:	lea    (%r14,%rdi,1),%r13
  39d580:	vpsrlq $0x20,%ymm6,%ymm7
  39d585:	vpsrlq $0x20,%ymm5,%ymm8
  39d58a:	xor    %edi,%edi
  39d58c:	nopl   0x0(%rax)
  39d590:	vpermd (%r14,%rdi,8),%ymm0,%ymm9
  39d596:	vmovdqu 0x0(%r13,%rdi,8),%ymm10
  39d59d:	vpminud %xmm9,%xmm4,%xmm11
  39d5a2:	vpcmpeqd %xmm4,%xmm11,%xmm11
  39d5a6:	vpand  %xmm4,%xmm11,%xmm11
  39d5aa:	vpsubd %xmm11,%xmm9,%xmm9
  39d5af:	vpmuludq %ymm6,%ymm10,%ymm11
  39d5b3:	vpmuludq %ymm7,%ymm10,%ymm12
  39d5b7:	vpsllq $0x20,%ymm12,%ymm12
  39d5bd:	vpaddq %ymm12,%ymm11,%ymm11
  39d5c2:	vpsrlq $0x20,%ymm11,%ymm11
  39d5c8:	vpmuludq %ymm5,%ymm10,%ymm12
  39d5cc:	vpmuludq %ymm8,%ymm10,%ymm10
  39d5d1:	vpsllq $0x20,%ymm10,%ymm10
  39d5d7:	vpaddq %ymm10,%ymm12,%ymm10
  39d5dc:	vpmuludq %ymm3,%ymm11,%ymm11
  39d5e0:	vpsubq %ymm11,%ymm10,%ymm10
  39d5e5:	vpmovzxdq %xmm9,%ymm9
  39d5ea:	vpaddq %ymm9,%ymm10,%ymm11
  39d5ef:	vmovdqu %ymm11,(%r14,%rdi,8)
  39d5f5:	vpaddq %ymm2,%ymm9,%ymm9
  39d5f9:	vpsubq %ymm10,%ymm9,%ymm9
  39d5fe:	vmovdqu %ymm9,0x0(%r13,%rdi,8)
  39d605:	add    $0x4,%rdi
  39d609:	cmp    %rdi,%r8
  39d60c:	jne    39d590 <valar_spiral_rs::ntt::avx2::ntt_forward+0x5c0>
  39d60e:	mov    %r8,%r13
  39d611:	cmp    %r8,%r12
  39d614:	je     39d510 <valar_spiral_rs::ntt::avx2::ntt_forward+0x540>
  39d61a:	nopw   0x0(%rax,%rax,1)
  39d620:	mov    (%r14,%r13,8),%edi
  39d624:	mov    (%rax,%r13,8),%r11d
  39d628:	cmp    %edi,%ecx
  39d62a:	mov    %ecx,%r10d
  39d62d:	cmova  %ebp,%r10d
  39d631:	sub    %r10d,%edi
  39d634:	mov    %r11,%r10
  39d637:	imul   %r15,%r10
  39d63b:	shr    $0x20,%r10
  39d63f:	imul   %rsi,%r11
  39d643:	imul   %r9,%r10
  39d647:	sub    %r10,%r11
  39d64a:	lea    (%r11,%rdi,1),%r10
  39d64e:	mov    %r10,(%r14,%r13,8)
  39d652:	add    %rcx,%rdi
  39d655:	sub    %r11,%rdi
  39d658:	mov    %rdi,(%rax,%r13,8)
  39d65c:	inc    %r13
  39d65f:	cmp    %r13,%r12
  39d662:	jne    39d620 <valar_spiral_rs::ntt::avx2::ntt_forward+0x650>
  39d664:	jmp    39d510 <valar_spiral_rs::ntt::avx2::ntt_forward+0x540>
  39d669:	nopl   0x0(%rax)
  39d670:	mov    %r12,%rdx
  39d673:	shl    $0x4,%rdx
  39d677:	mov    (%rsp),%rsi
  39d67b:	xor    %r8d,%r8d
  39d67e:	xchg   %ax,%ax
  39d680:	add    %rbx,%r8
  39d683:	vpmovzxdq 0x0(%r13,%r8,8),%xmm5
  39d68a:	vpmovzxdq (%r15,%r8,8),%xmm6
  39d690:	mov    %rdi,%r8
  39d693:	vpbroadcastq %xmm5,%ymm5
  39d698:	vpbroadcastq %xmm6,%ymm6
  39d69d:	mov    %rsi,%r10
  39d6a0:	mov    %rax,%r14
  39d6a3:	data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  39d6b0:	vmovdqa (%r10),%ymm7
  39d6b5:	vmovdqa (%r10,%r12,8),%ymm8
  39d6bb:	vpcmpgtq %ymm2,%ymm7,%ymm9
  39d6c0:	vpand  %ymm2,%ymm9,%ymm9
  39d6c4:	vpsubq %ymm9,%ymm7,%ymm7
  39d6c9:	vpmuludq %ymm5,%ymm8,%ymm9
  39d6cd:	vpsrlq $0x20,%ymm5,%ymm10
  39d6d2:	vpmuludq %ymm10,%ymm8,%ymm10
  39d6d7:	vpsllq $0x20,%ymm10,%ymm10
  39d6dd:	vpaddq %ymm10,%ymm9,%ymm9
  39d6e2:	vpsrlq $0x20,%ymm9,%ymm9
  39d6e8:	vpmuludq %ymm6,%ymm8,%ymm10
  39d6ec:	vpsrlq $0x20,%ymm6,%ymm11
  39d6f1:	vpmuludq %ymm11,%ymm8,%ymm8
  39d6f6:	vpsllq $0x20,%ymm8,%ymm8
  39d6fc:	vpaddq %ymm8,%ymm10,%ymm8
  39d701:	vpmuludq %ymm3,%ymm9,%ymm9
  39d705:	vpsubq %ymm9,%ymm8,%ymm8
  39d70a:	vpaddq %ymm7,%ymm8,%ymm9
  39d70e:	vpaddq %ymm2,%ymm7,%ymm7
  39d712:	vpsubq %ymm8,%ymm7,%ymm7
  39d717:	vmovdqa %ymm9,(%r10)
  39d71c:	vmovdqa %ymm7,(%r10,%r12,8)
  39d722:	add    $0x20,%r10
  39d726:	dec    %r14
  39d729:	jne    39d6b0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x6e0>
  39d72b:	cmp    %rbx,%r8
  39d72e:	mov    %r8,%rdi
  39d731:	adc    $0x0,%rdi
  39d735:	add    %rdx,%rsi
  39d738:	cmp    %rbx,%r8
  39d73b:	jb     39d680 <valar_spiral_rs::ntt::avx2::ntt_forward+0x6b0>
  39d741:	jmp    39d460 <valar_spiral_rs::ntt::avx2::ntt_forward+0x490>
  39d746:	cs nopw 0x0(%rax,%rax,1)
  39d750:	cmp    $0xa,%r11
  39d754:	ja     39d7d0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x800>
  39d756:	mov    0x40(%rsp),%rbx
  39d75b:	cmp    $0x1,%ebx
  39d75e:	mov    0x8(%rsp),%r8
  39d763:	mov    0x50(%rsp),%rdi
  39d768:	mov    0x48(%rsp),%r10
  39d76d:	mov    0x38(%rsp),%r15
  39d772:	mov    0x70(%rsp),%r12
  39d777:	mov    0x78(%rsp),%rdx
  39d77c:	ja     39d83b <valar_spiral_rs::ntt::avx2::ntt_forward+0x86b>
  39d782:	xor    %eax,%eax
  39d784:	mov    (%rsp),%rdx
  39d788:	jmp    39d7aa <valar_spiral_rs::ntt::avx2::ntt_forward+0x7da>
  39d78a:	nopw   0x0(%rax,%rax,1)
  39d790:	sub    %r14,%rdx
  39d793:	mov    (%rsp),%r14
  39d797:	mov    %rdx,(%r14,%rax,8)
  39d79b:	mov    %r14,%rdx
  39d79e:	inc    %rax
  39d7a1:	cmp    %rax,%rsi
  39d7a4:	je     39d390 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3c0>
  39d7aa:	mov    (%rdx,%rax,8),%rdx
  39d7ae:	mov    $0x0,%r14d
  39d7b4:	cmp    %rcx,%rdx
  39d7b7:	jb     39d7bc <valar_spiral_rs::ntt::avx2::ntt_forward+0x7ec>
  39d7b9:	mov    %rcx,%r14
  39d7bc:	sub    %r14,%rdx
  39d7bf:	mov    $0x0,%r14d
  39d7c5:	cmp    %r9,%rdx
  39d7c8:	jb     39d790 <valar_spiral_rs::ntt::avx2::ntt_forward+0x7c0>
  39d7ca:	mov    %r9,%r14
  39d7cd:	jmp    39d790 <valar_spiral_rs::ntt::avx2::ntt_forward+0x7c0>
  39d7cf:	nop
  39d7d0:	cmpq   $0x0,0x30(%rsp)
  39d7d6:	mov    0x8(%rsp),%r8
  39d7db:	mov    0x50(%rsp),%rdi
  39d7e0:	mov    0x48(%rsp),%r10
  39d7e5:	mov    0x40(%rsp),%rbx
  39d7ea:	mov    0x38(%rsp),%r15
  39d7ef:	mov    (%rsp),%rdx
  39d7f3:	je     39d390 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3c0>
  39d7f9:	xor    %ecx,%ecx
  39d7fb:	mov    0x30(%rsp),%rax
  39d800:	cmp    %rsi,%rcx
  39d803:	jae    39dc6f <valar_spiral_rs::ntt::avx2::ntt_forward+0xc9f>
  39d809:	vmovdqa (%rdx,%rcx,8),%ymm4
  39d80e:	vpcmpgtq %ymm2,%ymm4,%ymm5
  39d813:	vpand  %ymm2,%ymm5,%ymm5
  39d817:	vpsubq %ymm5,%ymm4,%ymm4
  39d81b:	vpcmpgtq %ymm3,%ymm4,%ymm5
  39d820:	vpand  %ymm3,%ymm5,%ymm5
  39d824:	vpsubq %ymm5,%ymm4,%ymm4
  39d828:	vmovdqa %ymm4,(%rdx,%rcx,8)
  39d82d:	add    $0x4,%rcx
  39d831:	dec    %rax
  39d834:	jne    39d800 <valar_spiral_rs::ntt::avx2::ntt_forward+0x830>
  39d836:	jmp    39d390 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3c0>
  39d83b:	vpor   %ymm1,%ymm2,%ymm4
  39d83f:	vpor   %ymm1,%ymm3,%ymm5
  39d843:	cmp    $0x4,%ebx
  39d846:	jae    39d88c <valar_spiral_rs::ntt::avx2::ntt_forward+0x8bc>
  39d848:	xor    %eax,%eax
  39d84a:	mov    (%rsp),%rcx
  39d84e:	xchg   %ax,%ax
  39d850:	vmovdqu (%rcx,%rax,8),%ymm6
  39d855:	vpxor  %ymm1,%ymm6,%ymm7
  39d859:	vpcmpgtq %ymm7,%ymm4,%ymm7
  39d85e:	vpandn %ymm2,%ymm7,%ymm7
  39d862:	vpsubq %ymm7,%ymm6,%ymm6
  39d866:	vpxor  %ymm1,%ymm6,%ymm7
  39d86a:	vpcmpgtq %ymm7,%ymm5,%ymm7
  39d86f:	vpandn %ymm3,%ymm7,%ymm7
  39d873:	vpsubq %ymm7,%ymm6,%ymm6
  39d877:	vmovdqu %ymm6,(%rcx,%rax,8)
  39d87c:	add    $0x4,%rax
  39d880:	cmp    %rax,0x20(%rsp)
  39d885:	jne    39d850 <valar_spiral_rs::ntt::avx2::ntt_forward+0x880>
  39d887:	jmp    39d390 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3c0>
  39d88c:	add    0x68(%rsp),%rdx
  39d891:	xor    %eax,%eax
  39d893:	data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  39d8a0:	vmovdqu -0x60(%rdx,%rax,8),%ymm6
  39d8a6:	vmovdqu -0x40(%rdx,%rax,8),%ymm7
  39d8ac:	vmovdqu -0x20(%rdx,%rax,8),%ymm8
  39d8b2:	vmovdqu (%rdx,%rax,8),%ymm9
  39d8b7:	vpxor  %ymm1,%ymm6,%ymm10
  39d8bb:	vpcmpgtq %ymm10,%ymm4,%ymm10
  39d8c0:	vpandn %ymm2,%ymm10,%ymm10
  39d8c4:	vpxor  %ymm1,%ymm7,%ymm11
  39d8c8:	vpcmpgtq %ymm11,%ymm4,%ymm11
  39d8cd:	vpandn %ymm2,%ymm11,%ymm11
  39d8d1:	vpxor  %ymm1,%ymm8,%ymm12
  39d8d5:	vpcmpgtq %ymm12,%ymm4,%ymm12
  39d8da:	vpandn %ymm2,%ymm12,%ymm12
  39d8de:	vpxor  %ymm1,%ymm9,%ymm13
  39d8e2:	vpcmpgtq %ymm13,%ymm4,%ymm13
  39d8e7:	vpandn %ymm2,%ymm13,%ymm13
  39d8eb:	vpsubq %ymm10,%ymm6,%ymm6
  39d8f0:	vpsubq %ymm11,%ymm7,%ymm7
  39d8f5:	vpsubq %ymm12,%ymm8,%ymm8
  39d8fa:	vpsubq %ymm13,%ymm9,%ymm9
  39d8ff:	vpxor  %ymm1,%ymm6,%ymm10
  39d903:	vpcmpgtq %ymm10,%ymm5,%ymm10
  39d908:	vpandn %ymm3,%ymm10,%ymm10
  39d90c:	vpxor  %ymm1,%ymm7,%ymm11
  39d910:	vpcmpgtq %ymm11,%ymm5,%ymm11
  39d915:	vpandn %ymm3,%ymm11,%ymm11
  39d919:	vpxor  %ymm1,%ymm8,%ymm12
  39d91d:	vpcmpgtq %ymm12,%ymm5,%ymm12
  39d922:	vpandn %ymm3,%ymm12,%ymm12
  39d926:	vpxor  %ymm1,%ymm9,%ymm13
  39d92a:	vpcmpgtq %ymm13,%ymm5,%ymm13
  39d92f:	vpandn %ymm3,%ymm13,%ymm13
  39d933:	vpsubq %ymm10,%ymm6,%ymm6
  39d938:	vpsubq %ymm11,%ymm7,%ymm7
  39d93d:	vpsubq %ymm12,%ymm8,%ymm8
  39d942:	vpsubq %ymm13,%ymm9,%ymm9
  39d947:	vmovdqu %ymm6,-0x60(%rdx,%rax,8)
  39d94d:	vmovdqu %ymm7,-0x40(%rdx,%rax,8)
  39d953:	vmovdqu %ymm8,-0x20(%rdx,%rax,8)
  39d959:	vmovdqu %ymm9,(%rdx,%rax,8)
  39d95e:	add    $0x10,%rax
  39d962:	cmp    %rax,%r12
  39d965:	jne    39d8a0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x8d0>
  39d96b:	jmp    39d390 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3c0>
  39d970:	mov    0x68(%rsp),%rax
  39d975:	cmp    $0x1,%eax
  39d978:	mov    0x8(%rsp),%rsi
  39d97d:	ja     39da4b <valar_spiral_rs::ntt::avx2::ntt_forward+0xa7b>
  39d983:	xor    %eax,%eax
  39d985:	jmp    39d9a3 <valar_spiral_rs::ntt::avx2::ntt_forward+0x9d3>
  39d987:	nopw   0x0(%rax,%rax,1)
  39d990:	sub    %rdx,%rcx
  39d993:	mov    %rcx,(%rsi,%rax,8)
  39d997:	inc    %rax
  39d99a:	cmp    %rax,%r9
  39d99d:	je     39dbab <valar_spiral_rs::ntt::avx2::ntt_forward+0xbdb>
  39d9a3:	mov    (%rsi,%rax,8),%rcx
  39d9a7:	mov    $0x0,%edx
  39d9ac:	cmp    (%rsp),%rcx
  39d9b0:	jb     39d9b6 <valar_spiral_rs::ntt::avx2::ntt_forward+0x9e6>
  39d9b2:	mov    (%rsp),%rdx
  39d9b6:	sub    %rdx,%rcx
  39d9b9:	mov    $0x0,%edx
  39d9be:	cmp    0x28(%rsp),%rcx
  39d9c3:	jb     39d990 <valar_spiral_rs::ntt::avx2::ntt_forward+0x9c0>
  39d9c5:	mov    0x28(%rsp),%rdx
  39d9ca:	jmp    39d990 <valar_spiral_rs::ntt::avx2::ntt_forward+0x9c0>
  39d9cc:	cmp    %rdx,%r9
  39d9cf:	ja     39dc12 <valar_spiral_rs::ntt::avx2::ntt_forward+0xc42>
  39d9d5:	test   %rsi,%rsi
  39d9d8:	je     39dcdb <valar_spiral_rs::ntt::avx2::ntt_forward+0xd0b>
  39d9de:	mov    0x10(%rcx),%rax
  39d9e2:	test   %rax,%rax
  39d9e5:	je     39dc81 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcb1>
  39d9eb:	cmp    $0x1,%rax
  39d9ef:	je     39dc94 <valar_spiral_rs::ntt::avx2::ntt_forward+0xcc4>
  39d9f5:	mov    0xa8(%rdi),%rax
  39d9fc:	lea    (%rax,%rax,1),%rcx
  39da00:	xor    %edx,%edx
  39da02:	mov    0x8(%rsp),%rdi
  39da07:	jmp    39da28 <valar_spiral_rs::ntt::avx2::ntt_forward+0xa58>
  39da09:	nopl   0x0(%rax)
  39da10:	sub    %rdi,%rsi
  39da13:	mov    0x8(%rsp),%rdi
  39da18:	mov    %rsi,(%rdi,%rdx,8)
  39da1c:	inc    %rdx
  39da1f:	cmp    %rdx,%r9
  39da22:	je     39dbab <valar_spiral_rs::ntt::avx2::ntt_forward+0xbdb>
  39da28:	mov    (%rdi,%rdx,8),%rsi
  39da2c:	mov    $0x0,%edi
  39da31:	cmp    %rcx,%rsi
  39da34:	jb     39da39 <valar_spiral_rs::ntt::avx2::ntt_forward+0xa69>
  39da36:	mov    %rcx,%rdi
  39da39:	sub    %rdi,%rsi
  39da3c:	mov    $0x0,%edi
  39da41:	cmp    %rax,%rsi
  39da44:	jb     39da10 <valar_spiral_rs::ntt::avx2::ntt_forward+0xa40>
  39da46:	mov    %rax,%rdi
  39da49:	jmp    39da10 <valar_spiral_rs::ntt::avx2::ntt_forward+0xa40>
  39da4b:	vmovq  (%rsp),%xmm0
  39da50:	vpbroadcastq %xmm0,%ymm0
  39da55:	vmovq  0x28(%rsp),%xmm1
  39da5b:	vpbroadcastq %xmm1,%ymm1
  39da60:	cmp    $0x4,%eax
  39da63:	jae    39daba <valar_spiral_rs::ntt::avx2::ntt_forward+0xaea>
  39da65:	and    $0xc,%r9d
  39da69:	xor    %eax,%eax
  39da6b:	vpbroadcastq -0x35a854(%rip),%ymm2        # 43220 <GCC_except_table4170+0x213c>
  39da74:	vpxor  %ymm2,%ymm0,%ymm3
  39da78:	vpxor  %ymm2,%ymm1,%ymm4
  39da7c:	nopl   0x0(%rax)
  39da80:	vmovdqu (%rsi,%rax,8),%ymm5
  39da85:	vpxor  %ymm2,%ymm5,%ymm6
  39da89:	vpcmpgtq %ymm6,%ymm3,%ymm6
  39da8e:	vpandn %ymm0,%ymm6,%ymm6
  39da92:	vpsubq %ymm6,%ymm5,%ymm5
  39da96:	vpxor  %ymm2,%ymm5,%ymm6
  39da9a:	vpcmpgtq %ymm6,%ymm4,%ymm6
  39da9f:	vpandn %ymm1,%ymm6,%ymm6
  39daa3:	vpsubq %ymm6,%ymm5,%ymm5
  39daa7:	vmovdqu %ymm5,(%rsi,%rax,8)
  39daac:	add    $0x4,%rax
  39dab0:	cmp    %rax,%r9
  39dab3:	jne    39da80 <valar_spiral_rs::ntt::avx2::ntt_forward+0xab0>
  39dab5:	jmp    39dbab <valar_spiral_rs::ntt::avx2::ntt_forward+0xbdb>
  39daba:	and    $0xfffffffffffffff0,%r9
  39dabe:	xor    %eax,%eax
  39dac0:	vpbroadcastq -0x35a8a9(%rip),%ymm2        # 43220 <GCC_except_table4170+0x213c>
  39dac9:	vpxor  %ymm2,%ymm0,%ymm3
  39dacd:	vpxor  %ymm2,%ymm1,%ymm4
  39dad1:	data16 data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  39dae0:	vmovdqu (%rsi,%rax,8),%ymm5
  39dae5:	vmovdqu 0x20(%rsi,%rax,8),%ymm6
  39daeb:	vmovdqu 0x40(%rsi,%rax,8),%ymm7
  39daf1:	vmovdqu 0x60(%rsi,%rax,8),%ymm8
  39daf7:	vpxor  %ymm2,%ymm5,%ymm9
  39dafb:	vpcmpgtq %ymm9,%ymm3,%ymm9
  39db00:	vpandn %ymm0,%ymm9,%ymm9
  39db04:	vpxor  %ymm2,%ymm6,%ymm10
  39db08:	vpcmpgtq %ymm10,%ymm3,%ymm10
  39db0d:	vpandn %ymm0,%ymm10,%ymm10
  39db11:	vpxor  %ymm2,%ymm7,%ymm11
  39db15:	vpcmpgtq %ymm11,%ymm3,%ymm11
  39db1a:	vpandn %ymm0,%ymm11,%ymm11
  39db1e:	vpxor  %ymm2,%ymm8,%ymm12
  39db22:	vpcmpgtq %ymm12,%ymm3,%ymm12
  39db27:	vpandn %ymm0,%ymm12,%ymm12
  39db2b:	vpsubq %ymm9,%ymm5,%ymm5
  39db30:	vpsubq %ymm10,%ymm6,%ymm6
  39db35:	vpsubq %ymm11,%ymm7,%ymm7
  39db3a:	vpsubq %ymm12,%ymm8,%ymm8
  39db3f:	vpxor  %ymm2,%ymm5,%ymm9
  39db43:	vpcmpgtq %ymm9,%ymm4,%ymm9
  39db48:	vpandn %ymm1,%ymm9,%ymm9
  39db4c:	vpxor  %ymm2,%ymm6,%ymm10
  39db50:	vpcmpgtq %ymm10,%ymm4,%ymm10
  39db55:	vpandn %ymm1,%ymm10,%ymm10
  39db59:	vpxor  %ymm2,%ymm7,%ymm11
  39db5d:	vpcmpgtq %ymm11,%ymm4,%ymm11
  39db62:	vpandn %ymm1,%ymm11,%ymm11
  39db66:	vpxor  %ymm2,%ymm8,%ymm12
  39db6a:	vpcmpgtq %ymm12,%ymm4,%ymm12
  39db6f:	vpandn %ymm1,%ymm12,%ymm12
  39db73:	vpsubq %ymm9,%ymm5,%ymm5
  39db78:	vpsubq %ymm10,%ymm6,%ymm6
  39db7d:	vpsubq %ymm11,%ymm7,%ymm7
  39db82:	vpsubq %ymm12,%ymm8,%ymm8
  39db87:	vmovdqu %ymm5,(%rsi,%rax,8)
  39db8c:	vmovdqu %ymm6,0x20(%rsi,%rax,8)
  39db92:	vmovdqu %ymm7,0x40(%rsi,%rax,8)
  39db98:	vmovdqu %ymm8,0x60(%rsi,%rax,8)
  39db9e:	add    $0x10,%rax
  39dba2:	cmp    %rax,%r9
  39dba5:	jne    39dae0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xb10>
  39dbab:	add    $0xd8,%rsp
  39dbb2:	pop    %rbx
  39dbb3:	pop    %r12
  39dbb5:	pop    %r13
  39dbb7:	pop    %r14
  39dbb9:	pop    %r15
  39dbbb:	pop    %rbp
  39dbbc:	vzeroupper
  39dbbf:	ret
  39dbc0:	lea    0x1f141(%rip),%rdx        # 3bcd08 <tokio::runtime::task::waker::WAKER_VTABLE+0x1010>
  39dbc7:	call   1a4b60 <core::panicking::panic_bounds_check>
  39dbcc:	lea    0x1f11d(%rip),%rdx        # 3bccf0 <tokio::runtime::task::waker::WAKER_VTABLE+0xff8>
  39dbd3:	mov    %rbp,%rdi
  39dbd6:	call   1a4b60 <core::panicking::panic_bounds_check>
  39dbdb:	lea    0x1f61e(%rip),%rax        # 3bd200 <tokio::runtime::task::waker::WAKER_VTABLE+0x1508>
  39dbe2:	mov    %rax,0xa8(%rsp)
  39dbea:	vmovaps -0x35c392(%rip),%ymm0        # 41860 <GCC_except_table4170+0x77c>
  39dbf2:	vmovups %ymm0,0xb0(%rsp)
  39dbfb:	lea    0x1f08e(%rip),%rsi        # 3bcc90 <tokio::runtime::task::waker::WAKER_VTABLE+0xf98>
  39dc02:	lea    0xa8(%rsp),%rdi
  39dc0a:	vzeroupper
  39dc0d:	call   1a10e0 <core::panicking::panic_fmt>
  39dc12:	lea    0x1f107(%rip),%rcx        # 3bcd20 <tokio::runtime::task::waker::WAKER_VTABLE+0x1028>
  39dc19:	xor    %edi,%edi
  39dc1b:	mov    %r9,%rsi
  39dc1e:	call   1a23c0 <core::slice::index::slice_index_fail>
  39dc23:	lea    0x1f0ae(%rip),%rdi        # 3bccd8 <tokio::runtime::task::waker::WAKER_VTABLE+0xfe0>
  39dc2a:	call   1a1100 <core::option::unwrap_failed>
  39dc2f:	mov    0x20(%rsp),%rax
  39dc34:	cmp    %rdx,%rax
  39dc37:	cmova  %rax,%rdx
  39dc3b:	mov    %rdx,%rdi
  39dc3e:	lea    0x1f063(%rip),%rdx        # 3bcca8 <tokio::runtime::task::waker::WAKER_VTABLE+0xfb0>
  39dc45:	mov    0x20(%rsp),%rsi
  39dc4a:	call   1a4b60 <core::panicking::panic_bounds_check>
  39dc4f:	mov    0x18(%rsp),%rax
  39dc54:	cmp    %rdx,%rax
  39dc57:	cmova  %rax,%rdx
  39dc5b:	mov    %rdx,%rdi
  39dc5e:	lea    0x1f05b(%rip),%rdx        # 3bccc0 <tokio::runtime::task::waker::WAKER_VTABLE+0xfc8>
  39dc65:	mov    0x18(%rsp),%rsi
  39dc6a:	call   1a4b60 <core::panicking::panic_bounds_check>
  39dc6f:	lea    0x1f182(%rip),%rdx        # 3bcdf8 <tokio::runtime::task::waker::WAKER_VTABLE+0x1100>
  39dc76:	mov    %rcx,%rdi
  39dc79:	vzeroupper
  39dc7c:	call   1a4b60 <core::panicking::panic_bounds_check>
  39dc81:	lea    0x1f600(%rip),%rdx        # 3bd288 <tokio::runtime::task::waker::WAKER_VTABLE+0x1590>
  39dc88:	xor    %edi,%edi
  39dc8a:	xor    %esi,%esi
  39dc8c:	vzeroupper
  39dc8f:	call   1a4b60 <core::panicking::panic_bounds_check>
  39dc94:	lea    0x1f635(%rip),%rdx        # 3bd2d0 <tokio::runtime::task::waker::WAKER_VTABLE+0x15d8>
  39dc9b:	mov    $0x1,%edi
  39dca0:	mov    $0x1,%esi
  39dca5:	vzeroupper
  39dca8:	call   1a4b60 <core::panicking::panic_bounds_check>
  39dcad:	lea    0x1f5bc(%rip),%rdx        # 3bd270 <tokio::runtime::task::waker::WAKER_VTABLE+0x1578>
  39dcb4:	mov    %rcx,%rdi
  39dcb7:	mov    0x58(%rsp),%rsi
  39dcbc:	vzeroupper
  39dcbf:	call   1a4b60 <core::panicking::panic_bounds_check>
  39dcc4:	lea    0x1f115(%rip),%rdx        # 3bcde0 <tokio::runtime::task::waker::WAKER_VTABLE+0x10e8>
  39dccb:	mov    $0x4,%esi
  39dcd0:	mov    %rcx,%rdi
  39dcd3:	vzeroupper
  39dcd6:	call   1a4b60 <core::panicking::panic_bounds_check>
  39dcdb:	lea    0x1f58e(%rip),%rdx        # 3bd270 <tokio::runtime::task::waker::WAKER_VTABLE+0x1578>
  39dce2:	xor    %edi,%edi
  39dce4:	call   1a4b60 <core::panicking::panic_bounds_check>
